.. _v0_configuration:

*************
Configuration
*************

Configuration files make it easy to quickly set up :class:`Broker` instances
with the expression ``Broker.named('example')`` where 'example' is the name of
a configuration file.

Search Path
-----------

The databroker looks for configuration files in three locations, in this order:

* ``~/.config/databroker`` (under the user's home directory)
* ``python/../etc/databroker``, where ``python`` is the current Python binary
  reported by ``sys.executable`` (This allows config to be provided inside a
  virtual environment.)
* ``/etc/databroker/``

A configuration file must be located in one of these directories, and it must
be named with the extension ``.yml``. Configuration files are formatted as YAML
files.

Examples
--------

This configuration file sets up a simple databroker backed by sqlite files.
This can be used immediately with no extra setup or installation.

.. code-block:: yaml

    description: 'lightweight personal database'
    metadatastore:
        module: 'databroker.headersource.sqlite'
        class: 'MDS'
        config:
            directory: 'some_directory'
            timezone: 'US/Eastern'
    assets:
        module: 'databroker.assets.sqlite'
        class: 'Registry'
        config:
            dbpath: 'some_directory/assets.sqlite'

This configuration file sets up a databroker that connects to a MongoDB server.
This requires more work to set up.

.. code-block:: yaml

    description: 'heavyweight shared database'
    metadatastore:
        module: 'databroker.headersource.mongo'
        class: 'MDS'
        config:
            host: 'localhost'
            port: 27017
            database: 'some_example_database'
            timezone: 'US/Eastern'
    assets:
        module: 'databroker.assets.mongo'
        class: 'Registry'
        config:
            host: 'localhost'
            port: 27017
            database: 'some_example_database'

In these examples, the classes used happen to be from the ``databroker``
package itself, but classes from other packages can be used just as well, as
long as they present the expected API. (This API is not yet documented outside
of the source code.)

Registering Handlers
--------------------

Configuration files may optionally include a section specifying 'handlers',
classes that load externally stored data. These may be registered at runtime or
here in configuration.

.. code-block:: yaml

    handlers:
        FOO:
            module: 'databroker.assets.path_only_handlers'
            class: 'RawHandler'

Coping with Moved Files or Different Mount Points
-------------------------------------------------

Optionally, you may set a root_map, which comes in handy when the handlers
involves mounted files that have been moved to a different mount point in the
file system.

.. code-block:: yaml

    root_map:
        old_root: new_root
        old_root2: new_root2

where ``old_root`` and ``old_root2`` are the old mount points and ``new_root``
and ``new_root2`` their respective new mount points.

.. warning::

    Future versions of databroker will provide better support for multiple
    asset registries and multiple sources of Event data, and this configuration
    file format will change. If possible, old configuration files will still be
    supported.

Helper Functions
----------------

See :ref:`configuration_utilities` in the API documentation.
