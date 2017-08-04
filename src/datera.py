# Copyright 2017 Datera Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Storage backend for Datera EDF storage system"""

import contextlib
import functools
import hashlib
import io
import json
import logging
import math
import os
import re
import shlex
import time
import uuid

import eventlet
from eventlet.green import threading
import requests
import six
from six.moves import http_client

from oslo_concurrency import processutils as putils
from oslo_config import cfg
from oslo_utils import excutils
from oslo_utils import units

import glance_store
from glance_store.i18n import _
from glance_store import capabilities
from glance_store import exceptions
from glance_store.common import utils

from os_brick.initiator import connector as os_conn
from os_brick import exception as brick_exception


CONF = cfg.CONF
LOG = logging.getLogger(__name__)

_DATERA_OPTS = [
    cfg.StrOpt('datera_san_ip',
               default=None,
               required=True,
               help='(REQUIRED) IP address of Datera EDF backend'),
    cfg.StrOpt('datera_san_login',
               default=None,
               required=True,
               help='(REQUIRED) Username for Datera EDF backend account'),
    cfg.StrOpt('datera_san_password',
               default=None,
               required=True,
               help='(REQUIRED) Password for Datera EDF backend account'),
    cfg.PortOpt('datera_san_port',
                default=7718,
                help='Port for connection to the Datera EDF backend account'),
    cfg.StrOpt('datera_tenant_id',
               default='/root',
               help='Datera tenant_id under which images should be stored'),
    cfg.IntOpt('datera_replica_count',
               default=3,
               help='Number of replicas to use for backing volumes'),
    cfg.StrOpt('datera_placement_mode',
               default='hybrid',
               help="'single_flash' for single-flash-replica placement, "
                    "'all_flash' for all-flash-replica placement, "
                    "'hybrid' for hybrid placement"),
    cfg.IntOpt('datera_chunk_size',
               default=units.Gi,
               help="Chunk size (in bytes) used for reading and writing to "
                    "backing volume.  Larger chunk sizes will use more memory,"
                    " but will potentially write and read faster")]

STORAGE_NAME = 'storage-1'
VOLUME_NAME = 'volume-1'
OS_PREFIX = "OS-"
URI_RE = re.compile(
    r"datera://(?P<hostname>.*?):(?P<port>\d+?)(?P<location>/app_instances/"
    r"{}(?P<image_id>.*$))".format(OS_PREFIX))
URL_TEMPLATES = {
    'ai': lambda: 'app_instances',
    'ai_inst': lambda: (URL_TEMPLATES['ai']() + '/{}'),
    'si': lambda: (URL_TEMPLATES['ai_inst']() + '/storage_instances'),
    'si_inst': lambda storage_name: (
        (URL_TEMPLATES['si']() + '/{}').format(
            '{}', storage_name)),
    'vol': lambda storage_name: (
        (URL_TEMPLATES['si_inst'](storage_name) + '/volumes')),
    'vol_inst': lambda storage_name, volume_name: (
        (URL_TEMPLATES['vol'](storage_name) + '/{}').format(
            '{}', volume_name)),
    'at': lambda: 'app_templates/{}'}

DEFAULT_SI_SLEEP = 1
INITIATOR_GROUP_PREFIX = "IG-"


class DateraAPIException(exceptions.GlanceStoreException):
    message = _("Error during communication with the Datera EDF backend")


def _get_name(name):
    return "".join((OS_PREFIX, name))


def _authenticated(func):
    """Ensure the driver is authenticated to make a request.

    In do_setup() we fetch an auth token and store it. If that expires when
    we do API request, we'll fetch a new one.
    """
    @functools.wraps(func)
    def func_wrapper(driver, *args, **kwargs):
        try:
            return func(driver, *args, **kwargs)
        except exceptions.NotAuthenticated:
            # Prevent recursion loop. After the driver arg is the
            # resource_type arg from _issue_api_request(). If attempt to
            # login failed, we should just give up.
            if args[0] == 'login':
                raise

            # Token might've expired, get a new one, try again.
            driver.login()
            return func(driver, *args, **kwargs)
    return func_wrapper


def _get_root_helper():
    return 'sudo glance-rootwrap %s' % CONF.glance_store.rootwrap_config


class DateraImage(object):
    """
    Datera images are not chunked.  They're stored on the backend device
    as a distributed volume, but from the perspective of an end-user they are
    a single block device
    """
    def __init__(self, image_id, host, port, location, driver):
        self.image_id = image_id
        self.host = host
        self.port = port
        self.location = location
        self.name = os.path.basename(self.location)
        self.driver = driver
        self._data_size = None
        self._vol_size = None

    @classmethod
    def create(cls, driver, image_id, image_file, image_size):
        """
        data_size must be in MiB
        """
        # Determine how large the volume should be to the nearest GB
        vol_size = int(math.ceil(float(image_size) / units.Gi))
        # Create image backing volume
        ai = driver.create_ai(image_id, vol_size)
        location = "/" + URL_TEMPLATES['ai_inst']().format(ai['name'])
        image = cls(image_id, driver.san_ip, driver.san_port, location, driver)
        # Copy image data to volume
        data_size, md5hex = driver.copy_image_to_vol(
            _get_name(image_id), image_file)
        return image, data_size, md5hex

    def read(self):
        reader = self.driver.read_image_from_vol(_get_name(self.image_id))
        size = next(reader)
        return reader, size

    @property
    def data_size(self):
        if self._data_size is None:
            pass
        return self._data_size

    @property
    def vol_size(self):
        if self._vol_size is None:
            self.driver.get_vol_size(self.location)
        return self._vol_size

    def get_uri(self):
        return StoreLocation({'image': self.image_id,
                              'host': self.host,
                              'port': self.port,
                              'location': self.location}, None).get_uri()


class StoreLocation(glance_store.location.StoreLocation):
    """
    Class describing a Datera EDF Image Location. This is of the form:

        datera://host:port:location
    """

    def process_specs(self):
        self.image_id = self.specs.get('image')
        self.host = self.specs.get('host')
        self.port = self.specs.get('port')
        self.location = self.specs.get('location')
        if not self.location:
            self.location = "/" + os.path.join("app_instances",
                                               _get_name(self.image_id))

    def parse_uri(self, uri):
        LOG.debug("Parsing uri: %s", uri)
        r = URI_RE.match(uri)
        self.image_id = r.group('image_id')
        self.host = r.group('hostname')
        self.port = r.group('port')
        self.location = r.group('location')

    def get_uri(self):
        return "datera://{host}:{port}{location}".format(
            host=self.host, port=self.port, location=self.location)


class Store(glance_store.driver.Store):

    _CAPABILITIES = capabilities.BitMasks.ALL

    OPTIONS = _DATERA_OPTS
    EXAMPLE_URL = "datera://hostname:port/location"

    def _image_from_location(self, loc):
        return DateraImage(
            loc.image_id, loc.host, loc.port, loc.location, self.driver)

    def configure_add(self):
        """
        Configure the store to use the stored configuration options
        and initialize capabilities based on current configuration.

        Any store that needs special configuration should implement
        this method.
        """
        try:
            self.driver = DateraDriver(
                self.conf.glance_store.datera_san_ip,
                self.conf.glance_store.datera_san_login,
                self.conf.glance_store.datera_san_password,
                self.conf.glance_store.datera_san_port,
                self.conf.glance_store.datera_tenant_id,
                self.conf.glance_store.datera_replica_count,
                self.conf.glance_store.datera_placement_mode,
                self.conf.glance_store.datera_chunk_size)
        except cfg.ConfigFileValueError as e:
            reason = _("Error in Datera store configuration: %s") % e
            raise exceptions.BadStoreConfiguration(
                store_name='datera',
                reason=reason)

    def get_schemes(self):
        """
        Returns a tuple of schemes which this store can handle.
        """
        return ('datera',)

    @capabilities.check
    def get(self, location, offset=0, chunk_size=None, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file, and returns a tuple of generator
        (for reading the image file) and image_size

        :param location: `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises: `glance.exceptions.NotFound` if image does not exist
        """
        LOG.debug("get() called with location: %s", location)
        try:
            image = self._image_from_location(location.store_location)
            return image.read()
        except Exception as e:
            # Logging exceptions here because Glance has a tendancy to
            # suppress them
            LOG.error(e, exc_info=1)
            raise

    def get_size(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file, and returns the size

        :param location: `glance_store.location.Location` object, supplied
                        from glance_store.location.get_location_from_uri()
        :raises: `glance_store.exceptions.NotFound` if image does not exist
        """
        LOG.debug("get_size() called with location: %s", location)
        try:
            image = self._image_from_location(location.store_location)
            return image.data_size
        except Exception as e:
            # Logging exceptions here because Glance has a tendancy to
            # suppress them
            LOG.error(e, exc_info=1)
            raise

    @capabilities.check
    def add(self, image_id, image_file, image_size, context=None,
            verifier=None):
        """
        Stores an image file with supplied identifier to the backend
        storage system and returns a tuple containing information
        about the stored image.

        :param image_id: The opaque image identifier
        :param image_file: The image data to write, as a file-like object
        :param image_size: The size of the image data to write, in bytes

        :retval: tuple of URL in backing store, bytes written, checksum
               and a dictionary with storage system specific information
        :raises: `glance_store.exceptions.Duplicate` if the image already
                existed
        """
        LOG.debug("add() called with image_id: %s", image_id)
        try:
            image, data_size, md5hex = DateraImage.create(
                self.driver, image_id, image_file, image_size)
            return image.get_uri(), data_size, md5hex, {}
        except Exception as e:
            # Logging exceptions here because Glance has a tendancy to
            # suppress them
            LOG.error(e, exc_info=1)
            raise

    @capabilities.check
    def delete(self, location, context=None):
        """
        Takes a `glance_store.location.Location` object that indicates
        where to find the image file to delete

        :param location: `glance_store.location.Location` object, supplied
                  from glance_store.location.get_location_from_uri()
        :raises: `glance_store.exceptions.NotFound` if image does not exist
        """
        LOG.debug("delete() called with location: %s", location)
        try:
            image = self._image_from_location(location.store_location)
            self.driver.delete_ai(_get_name(image.image_id))
        except Exception as e:
            # Logging exceptions here because Glance has a tendancy to
            # suppress them
            LOG.error(e, exc_info=1)
            raise


class DateraDriver(object):

    VERSION = '1.0.0'
    HEADER_DATA = {'Datera-Driver': 'OpenStack-Glance-{}'.format(VERSION)}
    API_VERSION = "2.1"

    def __init__(self, san_ip, username, password, port, tenant, replica_count,
                 placement_mode, chunk_size, ssl=True, client_cert=None,
                 client_cert_key=None):
        self.san_ip = san_ip
        self.username = username
        self.password = password
        self.san_port = port
        self.tenant_id = tenant
        self.replica_count = replica_count
        self.placement_mode = placement_mode
        self.chunk_size = chunk_size
        self.use_ssl = ssl
        self.datera_api_token = None
        self.thread_local = threading.local()
        self.thread_local.trace_id = None
        self.client_cert = client_cert
        self.client_cert_key = client_cert_key
        self.driver_prefix = str(uuid.uuid4())[:4]
        self.do_profile = True
        self.retry_attempts = 5
        self.interval = 2

    def login(self):
        """Use the san_login and san_password to set token."""
        body = {
            'name': self.username,
            'password': self.password
        }

        # Unset token now, otherwise potential expired token will be sent
        # along to be used for authorization when trying to login.
        self.datera_api_token = None

        try:
            LOG.debug('Getting Datera auth token.')
            results = self._issue_api_request(
                'login', 'put', body=body, sensitive=True)
            self.datera_api_token = results['key']
        except exceptions.NotAuthenticated:
            with excutils.save_and_reraise_exception():
                LOG.error('Logging into the Datera cluster failed. Please '
                          'check your username and password set in the '
                          'cinder.conf and start the cinder-volume '
                          'service again.')

    def get_metadata(self, ai_name):
        url = URL_TEMPLATES['ai_inst']().format(ai_name) + "/metadata"
        return self._issue_api_request(url)['data']

    def update_metadata(self, ai_name, keys):
        url = URL_TEMPLATES['ai_inst']().format(ai_name) + "/metadata"
        self._issue_api_request(url, method='put', body=keys)

    def create_ai(self, uid, size):
        app_params = (
            {
                'create_mode': "openstack",
                'uuid': uid,
                'name': _get_name(uid),
                'access_control_mode': 'deny_all',
                'storage_instances': [
                    {
                        'name': STORAGE_NAME,
                        'volumes': [
                            {
                                'name': VOLUME_NAME,
                                'size': size,
                                'placement_mode': self.placement_mode,
                                'replica_count': self.replica_count,
                                'snapshot_policies': [
                                ]
                            }
                        ]
                    }
                ]
            })
        url = URL_TEMPLATES['ai']()
        ai = self._issue_api_request(
            url, method='post', body=app_params)['data']
        return ai

    def detach_ai(self, ai_name):
        url = URL_TEMPLATES['ai_inst']().format(ai_name)
        data = {
            'admin_state': 'offline',
            'force': True
        }
        try:
            self._issue_api_request(url, method='put', body=data)
        except exceptions.NotFound:
            msg = _("Tried to detach volume %s, but it was not found in the "
                    "Datera cluster. Continuing with detach.")
            LOG.info(msg, ai_name)

    def delete_ai(self, ai_name):
        self.detach_ai(ai_name)
        try:
            self._issue_api_request(
                URL_TEMPLATES['ai_inst']().format(ai_name), 'delete')
        except (DateraAPIException, exceptions.NotFound):
            LOG.error(_("Couldn't find volume: %s"), ai_name)
            raise

    def get_vol_size(self, ai_name):
        url = os.path.join(ai_name, STORAGE_NAME, VOLUME_NAME)
        return self._issue_api_request(url)['data']['size']

    def read_image_from_vol(self, ai_name):
        # read metadata
        metadata = self.get_metadata(ai_name)
        len_data = int(metadata['length'])
        md5hex = metadata['checksum']
        check = hashlib.md5()
        yield len_data
        with self._connect_target(ai_name) as device:
            self._execute("chmod o+r {}".format(device))
            with io.open(device, 'rb') as infile:
                cur_data = len_data
                while True:
                    data = None
                    if cur_data < self.chunk_size:
                        data = infile.read(cur_data)
                        # Verify data integrity
                        check.update(data)
                        assert check.hexdigest() == md5hex
                        yield data
                        break
                    else:
                        data = infile.read(self.chunk_size)
                        cur_data -= self.chunk_size
                        check.update(data)
                        yield data
            self._execute("chmod o-r {}".format(device))

    def copy_image_to_vol(self, ai_name, image_file):
        md5 = hashlib.md5()
        len_data = 0
        with self._connect_target(ai_name) as device:
            self._execute("chmod o+w {}".format(device))
            chunks = utils.chunkreadable(image_file, self.chunk_size)
            with io.open(device, 'wb') as outfile:
                for data in chunks:
                    # Write image data
                    # Write number, length and MD5 to initial offset
                    len_data += len(data)
                    md5.update(data)
                    outfile.write(data)
                    LOG.debug(_("Writing Data.\n"
                                "Length: %s" % len(data)))
            self._execute("chmod o-w {}".format(device))
        md5hex = md5.hexdigest()
        # Add data length and checksum to ai metadata
        self.update_metadata(ai_name, {'checksum': md5hex, 'length': len_data})
        return len_data, md5hex

    def _get_sis_iqn_portal(self, ai_name):
        iqn = None
        portal = None
        url = URL_TEMPLATES['ai_inst']().format(ai_name)
        data = {
            'admin_state': 'online'
        }
        app_inst = self._issue_api_request(
            url, method='put', body=data)['data']
        storage_instances = app_inst["storage_instances"]
        si = storage_instances[0]
        portal = si['access']['ips'][0] + ':3260'
        iqn = si['access']['iqn']
        return storage_instances, iqn, portal

    def _register_acl(self, ai_name, initiator, storage_instances):
        initiator_name = "OpenStack_{}_{}".format(
            self.driver_prefix, str(uuid.uuid4())[:4])
        found = False
        if not found:
            data = {'id': initiator, 'name': initiator_name}
            # Try and create the initiator
            # If we get a conflict, ignore it
            self._issue_api_request("initiators",
                                    method="post",
                                    body=data,
                                    conflict_ok=True)
        initiator_path = "/initiators/{}".format(initiator)
        # Create ACL with initiator for storage_instances
        for si in storage_instances:
            acl_url = (URL_TEMPLATES['si']() +
                       "/{}/acl_policy").format(ai_name, si['name'])
            existing_acl = self._issue_api_request(acl_url, method="get")[
                'data']
            data = {}
            data['initiators'] = existing_acl['initiators']
            data['initiators'].append({"path": initiator_path})
            data['initiator_groups'] = existing_acl['initiator_groups']
            self._issue_api_request(acl_url, method="put", body=data)
        self._si_poll(ai_name)

    def _si_poll(self, bname):
        TIMEOUT = 10
        retry = 0
        check_url = URL_TEMPLATES['si_inst'](STORAGE_NAME).format(bname)
        poll = True
        while poll and not retry >= TIMEOUT:
            retry += 1
            si = self._issue_api_request(check_url)['data']
            if si['op_state'] == 'available':
                poll = False
            else:
                eventlet.sleep(1)
        if retry >= TIMEOUT:
            raise exceptions.BackendException(
                message=_('Resource not ready.'))

    @contextlib.contextmanager
    def _connect_target(self, ai_name):
        connector = None
        try:
            sis, iqn, portal = self._get_sis_iqn_portal(ai_name)
            conn = {'driver_volume_type': 'iscsi',
                    'data': {
                        'target_discovered': False,
                        'target_iqn': iqn,
                        'target_portal': portal,
                        'target_lun': 0,
                        'volume_id': None,
                        'discard': False}}
            connector = os_conn.InitiatorConnector.factory(
                conn['driver_volume_type'],
                _get_root_helper(),
                use_multipath=False,
                device_scan_attempts=10,
                conn=conn)

            # Setup ACL
            initiator = connector.get_initiator()
            self._register_acl(ai_name, initiator, sis)

            # Attach Target
            attach_info = {}
            attach_info['target_portal'] = portal
            attach_info['target_iqn'] = iqn
            attach_info['target_lun'] = 0
            retries = 10
            while True:
                try:
                    attach_info.update(
                        connector.connect_volume(conn['data']))
                    break
                except brick_exception.FailedISCSITargetPortalLogin:
                    retries -= 1
                    if not retries:
                        LOG.error(_("Could not log into portal before end of "
                                    "polling period"))
                        raise
                    LOG.debug("Failed to login to portal, retrying")
                    eventlet.sleep(2)
            device_path = attach_info['path']
            yield device_path
        finally:
            # Close target connection
            if connector:
                # Best effort disconnection
                try:
                    connector.disconnect_volume(attach_info, attach_info)
                except Exception:
                    pass

    def _handle_bad_status(self,
                           response,
                           connection_string,
                           method,
                           payload,
                           header,
                           cert_data,
                           sensitive=False,
                           conflict_ok=False):
        if (response.status_code == http_client.BAD_REQUEST and
                connection_string.endswith("api_versions")):
            # Raise the exception, but don't log any error.  We'll just fall
            # back to the old style of determining API version.  We make this
            # request a lot, so logging it is just noise
            raise DateraAPIException()
        if response.status_code == http_client.NOT_FOUND:
            raise exceptions.NotFound(response.json()['message'])
        elif response.status_code in [http_client.FORBIDDEN,
                                      http_client.UNAUTHORIZED]:
            raise exceptions.NotAuthenticated()
        elif response.status_code == http_client.CONFLICT and conflict_ok:
            # Don't raise, because we're expecting a conflict
            pass
        elif response.status_code == http_client.CONFLICT and not conflict_ok:
            raise exceptions.Duplicate()
        elif response.status_code == http_client.SERVICE_UNAVAILABLE:
            current_retry = 0
            while current_retry <= self.retry_attempts:
                LOG.debug("Datera 503 response, trying request again")
                eventlet.sleep(self.interval)
                resp = self._request(connection_string,
                                     method,
                                     payload,
                                     header,
                                     cert_data)
                if resp.ok:
                    return response.json()
                elif resp.status_code != http_client.SERVICE_UNAVAILABLE:
                    self._raise_response(resp)
        else:
            self._raise_response(response)

    @_authenticated
    def _issue_api_request(self, resource_url, method='get', body=None,
                           sensitive=False, conflict_ok=False):
        """All API requests to Datera cluster go through this method.

        :param resource_url: the url of the resource
        :param method: the request verb
        :param body: a dict with options for the action_type
        :param sensitive: Bool, whether request should be obscured from logs
        :param conflict_ok: Bool, True to suppress ConflictError exceptions
        during this request
        :returns: a dict of the response from the Datera cluster
        """
        api_version = self.API_VERSION
        host = self.san_ip
        port = self.san_port
        api_token = self.datera_api_token

        payload = json.dumps(body, ensure_ascii=False)
        payload.encode('utf-8')

        header = {'Content-Type': 'application/json; charset=utf-8'}
        header.update(self.HEADER_DATA)

        protocol = 'http'
        if self.use_ssl:
            protocol = 'https'

        if api_token:
            header['Auth-Token'] = api_token

        tenant = self.tenant_id
        if tenant == "all":
            header['tenant'] = tenant
        elif tenant and '/root' not in tenant:
            header['tenant'] = "".join(("/root/", tenant))
        elif tenant and '/root' in tenant:
            header['tenant'] = tenant
        elif self.tenant_id and self.tenant_id.lower() != "map":
            header['tenant'] = self.tenant_id

        client_cert = self.client_cert
        client_cert_key = self.client_cert_key
        cert_data = None

        if client_cert:
            protocol = 'https'
            cert_data = (client_cert, client_cert_key)

        connection_string = '%s://%s:%s/v%s/%s' % (protocol, host, port,
                                                   api_version, resource_url)

        request_id = uuid.uuid4()

        if self.do_profile:
            t1 = time.time()
        if not sensitive:
            LOG.debug("\nDatera Trace ID: %(tid)s\n"
                      "Datera Request ID: %(rid)s\n"
                      "Datera Request URL: /v%(api)s/%(url)s\n"
                      "Datera Request Method: %(method)s\n"
                      "Datera Request Payload: %(payload)s\n"
                      "Datera Request Headers: %(header)s\n",
                      {'tid': self.thread_local.trace_id,
                       'rid': request_id,
                       'api': api_version,
                       'url': resource_url,
                       'method': method,
                       'payload': payload,
                       'header': header})
        response = self._request(connection_string,
                                 method,
                                 payload,
                                 header,
                                 cert_data)

        data = response.json()

        timedelta = "Profiling disabled"
        if self.do_profile:
            t2 = time.time()
            timedelta = round(t2 - t1, 3)
        if not sensitive:
            LOG.debug("\nDatera Trace ID: %(tid)s\n"
                      "Datera Response ID: %(rid)s\n"
                      "Datera Response TimeDelta: %(delta)ss\n"
                      "Datera Response URL: %(url)s\n"
                      "Datera Response Payload: %(payload)s\n"
                      "Datera Response Object: %(obj)s\n",
                      {'tid': self.thread_local.trace_id,
                       'rid': request_id,
                       'delta': timedelta,
                       'url': response.url,
                       'payload': payload,
                       'obj': vars(response)})
        if not response.ok:
            self._handle_bad_status(response,
                                    connection_string,
                                    method,
                                    payload,
                                    header,
                                    cert_data,
                                    conflict_ok=conflict_ok)

        return data

    def _request(self, connection_string, method, payload, header, cert_data):
        LOG.debug("Endpoint for Datera API call: %s", connection_string)
        LOG.debug("Payload for Datera API call: %s", payload)
        try:
            response = getattr(requests, method)(connection_string,
                                                 data=payload, headers=header,
                                                 verify=False, cert=cert_data)
            return response
        except requests.exceptions.RequestException as ex:
            msg = _(
                'Failed to make a request to Datera cluster endpoint due '
                'to the following reason: %s') % six.text_type(
                ex.message)
            LOG.error(msg)
            raise DateraAPIException(msg)

    @staticmethod
    def _execute(cmd):
        parts = shlex.split(cmd)
        putils.execute(*parts, root_helper=_get_root_helper(),
                       run_as_root=True)