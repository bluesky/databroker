***********************
Creating a New Database
***********************

The databroker is above all an *interface*, which is not beholden to a specific
storage technology. Several different storage backends are included in the
databroker package (MongoDB, JSON, sqlite, HDF5) and fully tested. But as of
this writing, the maintainers of databroker rely exclusively on MongoDB for
large-scale production, and it has an additional production-oriented feature,
described here. Therefore, these instructions apply to the MongoDB storage
backend only. Brokers configured with other backends may ignore these
instructions.

Creation in MongoDB
-------------------

MongoDB itself does not require an explicit step for creating a new database.
It will quietly create one for you the first time you connect to it. This
can be dangerous (or, at best, messy) because an unnoticed typo can result in
an accidental, separate database. To enforce more explicit intention in this
process, databroker looks for a special "sentinel" collection when it connects
to a Mongo database. If it does not find one, it will refuse to connect to that
database until the user installs the sentinel --- thereby affirming, "Yes, I
intend to create a new database." The sentinel also contains a version number,
which may be useful for migrations in the event of any future changes to the
specification.

The creation of a database in MongoDB is simple. Simply create a configuration
as described in :doc:`configuration`. The only requirement is that the MongoDB
instance is running on the machine specified. The database need not exist yet.
The next step is to install a version sentinel. This is done as follows:

.. code-block:: python

    import databroker

    # Instantiate databroker instance
    from databroker import Broker
    db = Broker.named(config_name)

    # install sentinels
    databroker.assets.utils.install_sentinels(db.reg.config, version=1)

where ``config_name`` is the name of your configuration and ``version=1``
refers to the version of asset registry you are using (it is currently ``1`` as
of this writing).
