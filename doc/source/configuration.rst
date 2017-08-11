*************
Configuration
*************

Configuration files make it easy to quickly set up :class:`Broker` instances
with the expression ``Broker.named('example')`` where 'example' is the basename
of a configuration file.

Search Path
-----------

The databroker looks for configuration files in three locations, in this order:

* ``~/.config/databroker`` (under the user's home directory)
* ``python/../etc/databroker``, where ``python`` is the current Python binary
  reported by ``sys.executable`` (This allows config to be provided inside a virtual
  environment.)
* ``/etc/databroker/``

A configuration file must be located in one of these directories, and it must
have a filename ending in ``.yml``. Configuration files are formatted as YAML
files.

Examples
--------

This configuration file sets up a simple databroker backed by sqlite files.
This can be used immediately with no extra setup or installation.

.. code-block:: yaml

    'metadatastore': {
        'module': 'databroker.headersource.sqlite',
        'class': 'MDS',
        'config': {
            'directory': mds_dir}
    },
    'assets': {
        'module': 'databroker.assets.sqlite',
        'class': 'Registry',
        'config': {
            'dbpath': assets_dir + '/database.sql'}
    }

This configuration file sets up a databroker that connects to a MongoDB server.
This requires more work to set up.

.. code-block:: yaml

    'metadatastore': {
        'module': 'databroker.headersource.mongo',
        'class': 'MDS',
        'config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'mds_database_placholder',
            'timezone': 'US/Eastern'}
    },
    'assets': {
        'module': 'databroker.assets.mongo',
        'class': 'Registry',
        'config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'assets_database_placeholder'}
    }

.. warning::

    Future versions of databroker will provide better support for multiple
    asset registries and multiple sources of Event data, and this configuration
    file format will change. If possible, old configuration files will still be
    supported.

Helper Functions
----------------

See :ref:`configuration_utilities` in the API documentation.
