Configuration
=============

The databroker provides a user-friendly interface that unifies data from
multiple sources. It requires some initial configuration to locate all these
sources.

This configuration can be done in Python --- typically using a startup
file in an IPython profile so that they don't need to be typed every time.
Alternatively, the configuration can be specified in files or environment
variables. The file- or environment-based approach is less customizable, so by
default we recommend using a Python script.

Script-Based Configuration
--------------------------

1. Connect to metadatastore through an ``MDSRO`` ("metadata store read-only")
   instance.
2. Connect to filestore through a ``FileStoreRO`` ("file store read-only")
   instance.
3. Pass these two to ``Broker``, which provides a user-friendly interface the
   information in both of these together.

.. code-block:: python

    from metadatastore.mds import MDSRO  # "metadata store read-only"
    from filestore.fs import FileStoreRO  # "file store read-only"
    from databroker import Broker

    # This an example. You'll need to know your local configuration.
    mds = MSDRO({'host': 'localhost',
                 'port': 27017,
                 'database', 'metadatastore-production-v1',
                 'timezone': 'US/Eastern'})

    # This an example. You'll need to know your local configuration.
    fs = FileStoreRO({'host': 'localhost',
                      'port': 27017,
                      'database', 'filestore-production-v1')

    db = Broker(mds, fs)

Configuration Files or Environmental Variables
----------------------------------------------

DataBroker looks for configuration in:

``~/.config/metadatastore/connection.yml``
``/etc/metadatastore.yml``

in that order. Create a file like this in either of those locations:

.. code-block:: bash

    host: localhost
    port: 27017
    database: metadatastore-production-v1
    timezone: US/Eastern

Configuration can also be provided through the environmental variables which,
if set, take precedence over the files.

.. code-block:: bash

    export MDS_HOST=localhost
    export MDS_PORT=27017
    export MDS_DATABASE=metadatastore-production-v1
    export MDS_TIMEZONE=US/Eastern

Likewise, it looks in

``~/.config/filestore/connection.yml``
``/etc/filestore.yml``

for a file like:

.. code-block:: bash

    host: localhost
    port: 27017
    database: filestore-production-v1

which, likewise, can be overridden by environmental variables:

.. code-block:: bash

    export FS_HOST=localhost
    export FS_PORT=27017
    export FS_DATABASE=filestore-production-v1

Now connecting is as simple as:

.. code-block:: python

    from databroker import db

Under the hood, this locates the configuration, instantiates ``MDSRO`` and
``FileStoreRO`` using those parameters, and then instantiates ``Broker``, as
illustrated in the script-based configuration above.

If no configuration can be found, this will raise an error.
