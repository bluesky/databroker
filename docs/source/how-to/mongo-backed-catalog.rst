How to create a new Catalog backed by MongoDB
=============================================

#. Install the `MongoDB Community Edition`_. We recommend the latest stable
   version. Any version 3.x or later should be fine.

#. Find where Databroker looks for Catalog configuration files on your system.
   It varies by OS and environment because Databroker does its best to be a
   polite guest and place configuration files where the local conventions
   dictate. Run this snippet to find the list of paths where it looks
   on your system.

   .. code:: bash

      python3 -c "import databroker; print(databroker.catalog_search_path())"

#. Compose a configuration file like this. The filename of the configuration
   file is unimportant, but using ``CATALOG_NAME.yml`` is conventional. The
   file should be placed in any one of the directories listed by the previous
   step.

   .. code:: yaml

      sources:
        CATALOG_NAME:
          driver: bluesky-mongo-normalized-catalog
          args:
            metadatastore_db: mongodb://HOST:PORT/DATABASE_NAME
            asset_registry_db: mongodb://HOST:PORT/DATABASE_NAME

   where ``CATALOG_NAME`` is a name of the entry that will appear in
   ``databroker.catalog``. The two datbase URIs, ``metadatastore_db`` and
   ``asset_registry_db``, are distinct only for historical reasons. For new
   deployments, we recommend that you set them to the same value---i.e. that
   you use one database shared by both.

   If you are using Databroker on the same system where you are running
   MongoDB, then the URI would be ``mongodb://localhost:27017/DATABASE_NAME``
   where ``DATABASE_NAME`` is fully up to you.

#. Now ``CATALOG_NAME`` should appear in

   .. code:: python

      import databroker

      # List catalog names.
      list(datbroker.catalog)

   If it does not appear, call ``databroker.catalog.force_reload()`` and retry.
   The catalog may be accessed like

   .. code:: python

      catalog = databroker.catalog[CATALOG_NAME]

   using the ``CATALOG_NAME`` in the text of the configuration file. (Again,
   the *filename* of the configuration file is not relevant.)

See :doc:`store-data-from-run-engine` or
:doc:`store-analysis-results` to put some actual data in there, and see
the tutorials for how to get it back out.

Security
--------

.. note::

   This recommendation is a draft. If you are securing MongoDB, please reach
   out to us.

Datbroker was designed with access controls *per Run* in mind, and this is now
being actively developed, but currently only all-or-nothing access is
supported: Users can access all the Runs in the MongoDB or none of them.

#. `Enable authentication on MongoDB`_. Following those instructions, create a
   user with read and write access to your database and set a secure password.

#. Edit your configuration file as to add a template for username and password
   in the URI as follows.

   .. code:: yaml

      metadatastore_db: mongodb://{{ env(DATABROKER_MONGODB_USER) }}:{{ env(DATABROKER_MONGODB_PASSWORD) }}@HOST:PORT/DATABASE_NAME
      asset_registry_db: mongodb://{{ en(DATABROKER_MONGODB_USER) }}:{{ env(DATABROKER_MONGODB_PASSWORD) }}@HOST:PORT/DATABASE_NAME

#. Set these environment variables to provide access to the database.

   .. code:: bash

      export DATABROKER_MONGODB_USER='...'
      export DATABROKER_MONGODB_PASSWORD='...'

.. _MongoDB Community Edition: https://docs.mongodb.com/manual/administration/install-community/

.. _Enable authentication on MongoDB: https://docs.mongodb.com/manual/tutorial/enable-authentication/
