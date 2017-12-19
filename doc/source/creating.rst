***********************
Creating a New Database
***********************

In some cases, you may want to create a new database. Currently, storage and
retrieval is implemented in MongoDB so instructions for MongoDB will follow.
The implementation for other types of databases is likely similar.

Creation in MongoDB
-------------------

The creation of a database in MongoDB is simple. Simply create a configuration
as described in :doc:`configuration`. The only requirement is that the MongoDB
instance is running on the machine specified. The database need not exist yet.
The next step is to install a version sentinel. This is done as follows:

.. code-block:: python

    # Instantiate databroker instance
    from databroker import Broker
    db_analysis = Broker.named(config_name)

    # install sentinels
    databroker.assets.utils.install_sentinels(db_analysis.reg.config,
                                              version=1)

where ``config_name`` is the name of your configuration and ``version=1``
refers to the version of asset registry you are using (it is currently ``1`` as
of this writing).
