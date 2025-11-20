**********
Databroker
**********

|build_status| |coverage| |pypi_version| |license|

Deprecation Notice
==================

Databroker is no longer recommended for new users or facilities adopting
Bluesky. Instead, `Tiled`_ with `Bluesky Tiled Plugins`_ is recommended as the
canonical way to persist and access data and metadata from Bluesky.

Databroker now serves two purposes that remain relevant for some users and some
faciilities. First, it contains code adapting the legacy MongoDB-based Bluesky
document storage to Tiled---effectively a server-side plugin for Tiled. Second,
it wraps the Tiled Python client to provide an API backward-compatible with
legacy Databroker user code. _If you do not have MongoDB-based Bluesky storage
and you do not have legacy Databroker user code, you do not need Databroker._

Databroker will be maintained by NSLS-II through **April 2027** at minimum to
support the transition from MongoDB-based document storage to PostgreSQL-based
storage. The Python user interface may be maintained longer still, depending on
the need.

============== ==============================================================
PyPI           ``pip install databroker``
Conda          ``conda install -c conda-forge databroker``
Source code    https://github.com/bluesky/databroker
Documentation  https://blueskyproject.io/databroker
============== ==============================================================

The bundle of metadata and data looks like this, for example.

.. code:: python

   >>> run
   BlueskyRun
     uid='4a794c63-8223-4893-895e-d16e763188a8'
     exit_status='success'
     2020-03-07 09:17:40.436 -- 2020-03-07 09:28:53.173
     Streams:
       * primary
       * baseline

Additional user metadata beyond what is shown is stored in ``run.metadata``.
The bundle contains some number of logical tables of data ("streams"). They can
be accessed by name and read into a standard data structure from `xarray`_.
  
.. code:: python

    >>> run.primary.read()
    <xarray.Dataset>
    Dimensions:                   (time: 411)
    Coordinates:
      * time                      (time) float64 1.584e+09 1.584e+09 ... 1.584e+09
    Data variables:
        I0                        (time) float64 13.07 13.01 12.95 ... 9.862 9.845
        It                        (time) float64 11.52 11.47 11.44 ... 4.971 4.968
        Ir                        (time) float64 10.96 10.92 10.88 ... 4.761 4.763
        dwti_dwell_time           (time) float64 1.0 1.0 1.0 1.0 ... 1.0 1.0 1.0 1.0
        dwti_dwell_time_setpoint  (time) float64 1.0 1.0 1.0 1.0 ... 1.0 1.0 1.0 1.0
        dcm_energy                (time) float64 1.697e+04 1.698e+04 ... 1.791e+04
        dcm_energy_setpoint       (time) float64 1.697e+04 1.698e+04 ... 1.791e+04

Common search queries can be done with a high-level Python interface.

.. code:: python

    >>> from databroker.queries import TimeRange
    >>> catalog.search(TimeRange(since="2020"))

Custom queries can be done with the `MongoDB query language`_.

.. code:: python

    >>> query = {
    ...    "motors": {"$in": ["x", "y"]},  # scanning either x or y
    ...    "temperature" {"$lt": 300},  # temperature less than 300
    ...    "sample.element": "Ni",
    ... }
    >>> catalog.search(query)

See the tutorials for more.

.. |build_status| image:: https://github.com/bluesky/databroker/workflows/Unit%20Tests/badge.svg?branch=master
    :target: https://github.com/bluesky/databroker/actions?query=workflow%3A%22Unit+Tests%22
    :alt: Build Status

.. |coverage| image:: https://codecov.io/gh/bluesky/databroker/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/bluesky/databroker
    :alt: Test Coverage

.. |pypi_version| image:: https://img.shields.io/pypi/v/databroker.svg
    :target: https://pypi.org/project/databroker
    :alt: Latest PyPI version

.. |license| image:: https://img.shields.io/badge/License-BSD%203--Clause-blue.svg
    :target: https://opensource.org/licenses/BSD-3-Clause
    :alt: BSD 3-Clause License

.. _xarray: https://xarray.pydata.org/

.. _MongoDB query language: https://docs.mongodb.com/manual/reference/operator/query/

.. _Bluesky Data Model: https://blueskyproject.io/event-model/main/user/explanations/data-model.html

.. _Suitcase: https://blueskyproject.io/suitcase/

.. _Intake: https://intake.readthedocs.io/en/latest/

.. _Bluesky Tiled Plugins: https://blueskyproject.io/bluesky-tiled-plugins/

.. _Tiled: https://blueskyproject.io/tiled/
