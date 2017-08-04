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
     - Driver Branch Name
     - Driver Version
     - Capabilities Introduced
     - Supported Datera Product Versions
   * - Master
     - master
     - 1.0.0
     - Initial Driver
     - 2.1.X, 2.2.X

-------
Volume Driver Cinder.conf Options
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
   * - ``datera_chunk_size`` = 1 GB
     - (Int) Chunk size (in bytes) used for reading and writing to backing volumes.  Larger chunk sized will use more memory, but will potentially write and read faster

------
Installation
------

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

* Setup rootwrap

  If no ``rootwrap.conf`` or ``rootwrap.d`` exists, copy the contents of the
  ``etc/glance`` directory into the system's ``/etc/glance``.  If these files
  aleady exist, add the ``images.filters`` file to the
  ``/etc/glance/rootwrap.d/`` directory

* Restart the glance-api service
