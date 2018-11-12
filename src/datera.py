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
import hashlib
import io
import logging
import math
import os
import re
import shlex
import uuid

import eventlet
from eventlet.green import threading

import dfs_sdk

from oslo_concurrency import processutils as putils
from oslo_config import cfg
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
                    " but will potentially write and read faster"),
    cfg.StrOpt('datera_glance_rootwrap_path',
               default="sudo glance-rootwrap",
               help="Custom path and environment variables for calling "
                    "glance-rootwrap"),
    cfg.StrOpt('datera_glance_rootwrap_config',
               default="/etc/glance/rootwrap.conf",
               help="Custom path for rootwrap.conf file for glance-rootwrap"),
    cfg.IntOpt('datera_default_image_size',
               default=1,
               help="(GiB) This determines the starting image size if no size "
                    "is provided for an image on creation")]

STORAGE_NAME = 'storage-1'
VOLUME_NAME = 'volume-1'
OS_PREFIX = "OS-"
OS_POST_PREFIX = "IMAGE-"
URI_RE = re.compile(
    r"datera://(?P<hostname>.*?):(?P<port>\d+?)(?P<location>/app_instances/"
    r"({})?({})?(?P<image_id>.*$))".format(OS_PREFIX, OS_POST_PREFIX))
DEFAULT_SI_SLEEP = 1
API_VERSIONS = ["2.1", "2.2"]


def _get_name(name):
    return "".join((OS_PREFIX, OS_POST_PREFIX, name))


def _get_root_helper():
    return '%s %s' % (CONF.glance_store.datera_glance_rootwrap_path,
                      CONF.glance_store.datera_glance_rootwrap_config)


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
        self._vol_size = None

    @classmethod
    def create(cls, driver, image_id, image_file, image_size):
        """
        data_size must be in MiB
        """
        # Use default size if we're not given one
        incremental = False
        if image_size == 0:
            LOG.debug(_("Image size is 0, using default size of: %s, then "
                        "performing resize and extend for data in excess of "
                        "%s" % (driver.default_size, driver.default_size)))
            image_size = driver.default_size * units.Gi
            incremental = True
        # Determine how large the volume should be to the nearest GiB
        vol_size = int(math.ceil(float(image_size) / units.Gi))
        # We can't provision less than a single GiB volume
        if vol_size < 1:
            vol_size = 1
        # Create image backing volume
        ai = driver.create_ai(image_id, vol_size)
        location = '/'.join(ai['path'].split('/')[:-1] + [image_id])
        image = cls(image_id, driver.san_ip, driver.san_port, location, driver)
        # Copy image data to volume
        data_size, md5hex = driver.copy_image_to_vol(
            image_id, image_file, incremental)
        return image, data_size, md5hex

    def read(self):
        reader = self.driver.read_image_from_vol(self.image_id)
        size = next(reader)
        return reader, size

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
        LOG.debug(_("Parsing uri: %s" % uri))
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
                self.conf.glance_store.datera_chunk_size,
                self.conf.glance_store.datera_default_image_size)
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
        LOG.debug(_("get() called with location: %s, offset: %s, chunk_size: "
                    "%s, context: %s" %
                    (location, offset, chunk_size, context)))
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
        LOG.debug(_("get_size() called with location: %s, context: %s" %
                    (location, context)))
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
        LOG.debug(_("add() called with image_id: %s, image_file: %s, "
                    "image_size %s, context: %s, verifier: %s" %
                    (image_id, image_file, image_size, context, verifier)))
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
        LOG.debug("delete() called with location: %s, context: %s",
                  location, context)
        try:
            image = self._image_from_location(location.store_location)
            self.driver.delete_ai(image.image_id)
        except Exception as e:
            # Logging exceptions here because Glance has a tendancy to
            # suppress them
            LOG.error(e, exc_info=1)
            # We won't raise the exception after logging because we want
            # image deletion to fail gracefully.  We'll see why it failed
            # in the logs and not expose it to the user


class DateraDriver(object):

    VERSION = '2018.11.12.0'
    VERSION_HISTORY = """
        1.0.0 -- Initial driver
        1.0.1 -- Removing references to trace_id from driver
        1.0.2 -- Added datera_glance_rootwrap_path StrOpt and fixed
                 bug related to minimum volume size
        1.0.5 -- Rewrite of copy_image_to_vol to fix issues with copying
                 images to volumes
        1.0.6 -- Fixed deletion to fail gracefully. Removed unused constant.
        2018.4.19.0 -- Switched to date-based versioning scheme
        2018.7.18.0 -- Ported driver to Datera Python-SDK, changed initiators
                       to be created within a tenant rather than inherited
                       from the root tenant.  Changed naming convention to
                       OS-IMAGE-<image-id>
        2018.11.12.0 -- Fixed bug that broke support for v2.1 API-only versions
                        of the product
    """
    HEADER_DATA = {'Datera-Driver': 'OpenStack-Glance-{}'.format(VERSION)}

    def __init__(self, san_ip, username, password, port, tenant, replica_count,
                 placement_mode, chunk_size, default_image_size, ssl=True,
                 client_cert=None, client_cert_key=None):
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
        self.client_cert = client_cert
        self.client_cert_key = client_cert_key
        self.do_profile = True
        self.retry_attempts = 5
        self.interval = 2
        self.default_size = default_image_size

        if not all((self.san_ip, self.username, self.password)):
            raise exceptions.MissingCredentialError(required=[
                'datera_san_ip', 'datera_san_login', 'datera_san_password'])

        for apiv in reversed(API_VERSIONS):
            try:
                api = dfs_sdk.get_api(self.san_ip,
                                      self.username,
                                      self.password,
                                      'v{}'.format(apiv),
                                      disable_log=True)
                system = api.system.get()
                LOG.debug('Connected successfully to cluster: %s', system.name)
                self.api = api
                self.apiv = apiv
                break
            except Exception as e:
                LOG.warning(e)

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
        tenant = self._create_tenant()
        ai = self.api.app_instances.create(tenant=tenant, **app_params)
        return ai

    def detach_ai(self, ai_name):
        tenant = self._create_tenant()
        ai = self._name_to_ai(ai_name)
        data = {
            'admin_state': 'offline',
            'force': True
        }
        try:
            ai.set(tenant=tenant, **data)
        except dfs_sdk.exceptions.ApiNotFoundError:
            msg = _("Tried to detach volume %s, but it was not found in the "
                    "Datera cluster. Continuing with detach.")
            LOG.info(msg, ai_name)

    def delete_ai(self, ai_name):
        self.detach_ai(ai_name)
        tenant = self._create_tenant()
        ai = self._name_to_ai(ai_name)
        try:
            ai.delete(tenant=tenant)
        except dfs_sdk.exceptions.ApiNotFoundError:
            LOG.error(_("Couldn't find volume: %s"), ai_name)
            raise

    def get_vol_size(self, ai_name):
        vol = self._name_to_vol(ai_name)
        return vol.size

    def extend_vol(self, ai_name, size):
        vol = self._name_to_vol(ai_name)
        tenant = self._get_tenant()
        return vol.set(size=size, tenant=tenant)

    def rescan_device(self, device, size):
        """ Size must be in bytes """
        # Rescan ISCSI devices
        self._execute("iscsiadm -m session -R")
        result, _ = self._execute("blockdev --getsize64 %s" % device)
        new_size = int(result.strip())
        if new_size != size:
            raise EnvironmentError(_(
                "New blockdevice size does not match requested size, "
                "[%s != %s]" % size, new_size))

    def read_image_from_vol(self, ai_name):
        # read metadata
        ai = self._name_to_ai(ai_name)
        tenant = self._get_tenant()
        metadata = ai.metadata.get(tenant=tenant)
        len_data = int(metadata['length'])
        rlen_data = 0
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
                        rlen_data += len(data)
                        # Verify data integrity
                        check.update(data)
                        LOG.debug(_("Length Data. Metadata %s, Read %s" % (
                                  len_data, rlen_data)))
                        LOG.debug(_("MD5. Metadata %s, Read %s" % (
                                 md5hex, check.hexdigest())))
                        assert check.hexdigest() == md5hex
                        yield data
                        break
                    else:
                        data = infile.read(self.chunk_size)
                        rlen_data += len(data)
                        cur_data -= self.chunk_size
                        check.update(data)
                        yield data
            self._execute("chmod o-r {}".format(device))

    def copy_image_to_vol(self, ai_name, image_file, incremental):
        md5 = hashlib.md5()
        data_written = 0
        with self._connect_target(ai_name) as device:
            self._execute("chmod o+w {}".format(device))
            chunks = utils.chunkreadable(image_file, self.chunk_size)
            if incremental:
                vsize = self.get_vol_size(ai_name)  # In GiB
                for data in chunks:
                    len_data = len(data)
                    # bytes comparison
                    if len_data + data_written > vsize * units.Gi:
                        LOG.debug(_(
                            "Data %s exceeds volume size of %s extending "
                            "%s bytes" % (len_data + data_written,
                                          vsize * units.Gi,
                                          self.chunk_size)))
                        # In GiB
                        vsize += (self.chunk_size / units.Gi)
                        self.extend_vol(ai_name, vsize)
                        # Force the SCSI bus to scan for extended volume
                        self.rescan_device(device, vsize * units.Gi)
                    md5.update(data)
                    with io.open(device, 'wb') as outfile:
                        outfile.seek(data_written)
                        outfile.write(data)
                    data_written += len_data
                    LOG.debug(_("Writing Data. Length: %s" % len(data)))
                    self._execute("sync")
            else:
                with io.open(device, 'wb') as outfile:
                    for data in chunks:
                        # Write image data
                        data_written += len(data)
                        md5.update(data)
                        outfile.write(data)
                        LOG.debug(_("Writing Data. Length: %s, Offset: %s" %
                                  (len(data), outfile.tell())))
            self._execute("chmod o-w {}".format(device))
        md5hex = md5.hexdigest()
        # Add data length and checksum to ai metadata
        ai = self._name_to_ai(ai_name)
        ai.metadata.set(**{'checksum': md5hex,
                           'length': data_written,
                           'type': 'image'})
        return data_written, md5hex

    def _get_sis_iqn_portal(self, ai_name):
        data = {
            'admin_state': 'online'
        }
        ai = self._name_to_ai(ai_name)
        tenant = self._get_tenant()
        ai.set(tenant=tenant, **data)
        storage_instances = ai.storage_instances.list(tenant=tenant)
        si = storage_instances[0]
        portal = si.access['ips'][0] + ':3260'
        iqn = si.access['iqn']
        return storage_instances, iqn, portal

    def _register_acl(self, ai_name, initiator, storage_instances):
        initiator_name = "OpenStack-{}".format(str(uuid.uuid4())[:8])
        tenant = self._get_tenant()
        dinit = None
        try:
            # We want to make sure the initiator is created under the
            # current tenant rather than using the /root one
            dinit = self.api.initiators.get(initiator, tenant=tenant)
            if dinit.tenant != tenant:
                raise dfs_sdk.exceptions.ApiNotFoundError()
        except dfs_sdk.exceptions.ApiNotFoundError:
            # TODO(_alastor_): Take out the 'force' flag when we fix
            # DAT-15931
            data = {'id': initiator, 'name': initiator_name, 'force': True}
            # Try and create the initiator
            # If we get a conflict, ignore it
            try:
                dinit = self.api.initiators.create(tenant=tenant, **data)
            except dfs_sdk.exceptions.ApiConflictError:
                pass
        initiator_path = dinit['path']
        # Create ACL with initiator group as reference for each
        # storage_instance in app_instance
        # TODO(_alastor_): We need to avoid changing the ACLs if the
        # template already specifies an ACL policy.
        for si in storage_instances:
            existing_acl = si.acl_policy.get(tenant=tenant)
            data = {}
            # Grabbing only the 'path' key from each existing initiator
            # within the existing acl. eacli --> existing acl initiator
            eacli = []
            for acl in existing_acl['initiators']:
                nacl = {}
                nacl['path'] = acl['path']
                eacli.append(nacl)
            data['initiators'] = eacli
            data['initiators'].append({"path": initiator_path})
            # Grabbing only the 'path' key from each existing initiator
            # group within the existing acl. eaclig --> existing
            # acl initiator group
            eaclig = []
            for acl in existing_acl['initiator_groups']:
                nacl = {}
                nacl['path'] = acl['path']
                eaclig.append(nacl)
            data['initiator_groups'] = eaclig
            si.acl_policy.set(tenant=tenant, **data)
            self._si_poll(si, tenant)

    def _si_poll(self, si, tenant):
        TIMEOUT = 10
        retry = 0
        poll = True
        while poll and not retry >= TIMEOUT:
            retry += 1
            si = si.reload(tenant=tenant)
            if si.op_state == 'available':
                poll = False
            else:
                eventlet.sleep(1)
        if retry >= TIMEOUT:
            raise exceptions.BackendException(message=_('Resource not ready.'))

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
                # Best effort offline (cloning an offline AI is ~4 seconds
                # faster than cloning an online AI)
                try:
                    self.detach_ai(ai_name)
                except Exception:
                    pass

    @staticmethod
    def _execute(cmd):
        parts = shlex.split(cmd)
        stdout, stderr = putils.execute(*parts, root_helper=_get_root_helper(),
                                        run_as_root=True)
        return stdout, stderr

    @staticmethod
    def _format_tenant(tenant):
        if tenant == "all" or (
                tenant and ('/root' in tenant or 'root' in tenant)):
            return '/root'
        elif tenant and ('/root' not in tenant and 'root' not in tenant):
            return "/" + "/".join(('root', tenant)).strip('/')
        return tenant

    def _create_tenant(self):
        if self.tenant_id:
            name = self.tenant_id.replace('root', '').strip('/')
        else:
            name = 'root'
        if name:
            try:
                self.api.tenants.create(name=name)
            except dfs_sdk.exceptions.ApiConflictError:
                LOG.debug("Tenant {} already exists".format(name))
        return self._format_tenant(name)

    def _get_tenant(self):
        if not self.tenant_id:
            return self._format_tenant('root')
        return self._format_tenant(self.tenant_id)

    def _name_to_ai(self, name):
        tenant = self._get_tenant()
        try:
            # api.tenants.get needs a non '/'-prefixed tenant id
            self.api.tenants.get(tenant.strip('/'))
        except dfs_sdk.exceptions.ApiNotFoundError:
            self._create_tenant()
        ais = self.api.app_instances.list(
            filter='match(name,.*{}.*)'.format(name),
            tenant=tenant)
        if not ais:
            raise exceptions.NotFound(image=name)
        return ais[0]

    def _name_to_vol(self, name):
        ai = self._name_to_ai(name)
        tenant = self._get_tenant()
        si = ai.storage_instances.list(tenant=tenant)[0]
        return si.volumes.list(tenant=tenant)[0]
