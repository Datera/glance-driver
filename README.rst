=======
Datera Glance Repository
=======

------
Datera Glance Image Backend Driver
------

.. list-table:: Datera Glance Image Backend Driver Versions
   :header-rows: 1
   :class: config-ref-table

   * - OpenStack Release
     - Driver Tag/Branch Name
     - Driver Version
     - Capabilities Introduced
     - Supported Datera Product Versions
   * - Master
     - v2018.7.18.0
     - 2018.7.18.0
     - Ported driver to Datera Python-SDK, changed initiators to be created within a tenant rather than inherited from the root tenant.  Changed naming convention to OS-IMAGE-<image-id>
     - 2.2.X, 3.X+
   * - Master
     - v2018.4.19.0
     - 2018.4.19.0
     - Switched to date-based versioning scheme
     - 2.2.X, 3.X+
   * - Master
     - v1.0.6
     - 1.0.6
     - Fixed deletion to fail gracefully.  Removed unused constant
     - 2.1.X, 2.2.X, 3.X+
   * - Master
     - v1.0.5
     - 1.0.5
     - Rewrite of copy_image_to_vol to fix issues with copying images to volumes
     - 2.1.X, 2.2.X, 3.X+
   * - Master
     - v1.0.2
     - 1.0.2
     - Added datera_glance_rootwrap_path StrOpt and fixed bug related to minimum volume size
     - 2.1.X, 2.2.X, 3.X+
   * - Master
     - v1.0.1
     - 1.0.1
     - Bugfix for trace_id AttributeError
     - 2.1.X, 2.2.X, 3.X+
   * - Master
     - v1.0.0
     - 1.0.0
     - Initial Driver
     - 2.1.X, 2.2.X

-------
Volume Driver Glance-Api.conf Options
-------

.. list-table:: Description of Datera Glance backend driver configuration options
   :header-rows: 1
   :class: config-ref-table

   * - Configuration option = Default value
     - Description
   * - ``datera_san_ip`` = ``None``
     - (REQUIRED) (String) Datera EDF hostname/IP Address
   * - ``datera_san_login`` = ``None``
     - (REQUIRED) (String) Datera EDF username/login
   * - ``datera_san_password`` = ``None``
     - (REQUIRED) (String) Datera EDF username/login
   * - ``datera_san_port`` = ``7718``
     - (Int) Datera EDF connection port (7717 for http, 7718 for https)
   * - ``datera_tenant_id`` = ``/root``
     - (String) Datera tenant id/name under which images should be stored
   * - ``datera_replica_count`` = ``3``
     - (String) Number of replicas for created volumes
   * - ``datera_placement_mode`` = ``hybrid``
     - (String) 'single_flash' for single-flash-replica placement, 'all_flash' for all-flash-replica placement, 'hybrid' for hybrid placement
   * - ``datera_chunk_size`` = ``1073741824``
     - (Int) Chunk size (in bytes) used for reading and writing to backing volumes.  Larger chunk sized will use more memory, but will potentially write and read faster

------
Installation
------

* Install the driver

  Copy the contents of ``src`` into ``/usr/local/lib/python2.7/dist-packages/glance_store/_drivers/``

* Install the Datera Python-SDK

  ``sudo pip install -e git+http://github.com/Datera/python-sdk``

* Update the StrCfg in backend.py
  Modify the following in ``glance_store/backend.py``

.. code-block:: python

    cfg.StrOpt('default_store',
               default='file',
               choices=('file', 'filesystem', 'http', 'https', 'swift',
                        'swift+http', 'swift+https', 'swift+config', 'rbd',
                        'sheepdog', 'cinder', 'vsphere'),   # <-- add 'datera'

* Modify entry_points.txt

  This file is located in the ".egg" directory for the installation of
  glance_store.  It could be located in
  ``/usr/local/lib/python2.7/dist-packages/glance_store-X.XX.X.dist-info/entry_points.txt``

  If the following line is not present in entry_points.txt, go ahead and add it
  under [glance_store.drivers]:

  - ``datera = glance_store._drivers.datera:Store``

  This step is unecessary if the driver was obtained from the glance_store upstream repository

* Modify glance-api.conf

  Under ``[glance_store]`` modify the following values:
  ``stores = datera`` or if other stores should still be available add it to
  the list ``stores = file,datera``

  If Datera should be the default image store modify the ``default_store``
  value to: ``default_store = datera``.  There can only be one default store.

  Additionally set the required datera config options under ``[glance_store]``

  - ``datera_san_ip``
  - ``datera_san_login``
  - ``datera_san_password``

  If Datera is going to be used with a non ``/root`` tenant, then set
  - ``datera_tenant_id``
  Under ``[glance_store]`` as well

* Setup rootwrap

  If no ``rootwrap.conf`` or ``rootwrap.d`` exists, copy the contents of the
  ``etc/glance`` directory into the system's ``/etc/glance``.  If these files
  aleady exist, add the ``images.filters`` file to the
  ``/etc/glance/rootwrap.d/`` directory

* Restart the glance-api service
