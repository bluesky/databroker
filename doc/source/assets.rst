========
 Assets
========

The databroker includes an "asset registry" for interfacing transparently to
externally-stored data. Here's how it works.

#. You (or your hardware) save data wherever you want, whenever you want, in
   whatever format makes sense for your application.
#. At some point, you notify the Registry about this data. You provide two
   pieces of information.

   #. How to access and open the file (or, generically, "resource")
   #. How to retrieve a given piece of data within that file

#. In return, the Registry gives you a token ---  a unique identifier --- which
   you can use to retrieve each piece of data.

This admittedly abstract way of doing things has some powerful advantages.

* *Any* file format is supported, and retrieved data always comes back as a
  simple numpy array.
* It does not matter whether pieces of data are stored in one file or in many
  separate files.
* The registry does not get between the detector and the storage. It is
  not in the business of storing data; you merely *tell it about the data*, and
  you may do so before, during, or after the data is actually created or
  stored.

The following examples illustrate how this works in practice.

Example 1: Retrieving Lines from a CSV File
===========================================

Suppose my detector just saved a data file called ``example.csv``.

.. ipython:: python
   :suppress:

   # Generate an example file.
   import pandas as pd
   df = pd.DataFrame({'x': [1, 2, 3, 4, 5], 'y': [1, 4, 9, 16, 25]})
   df.to_csv('example.csv', index=False)

.. ipython:: python

   !cat example.csv

First, decide how fine-grained your access to the data should be. Would you
always want the whole file, or would like to be able to request data from
each line individually? Or each pair of lines? The "unit" of data is
completely flexible; it can be anything from a scalar to an N-dimensional
volume.

For this example, suppose we want to retrieve individual lines of data.

Write a Handler
---------------

A Handler is a class with two required methods,
an ``__init__`` that accesses a file and a ``__call__`` that returns nuggets of
data from that file. In our case, since we want to access individual lines of
the file, ``__call__`` will return a line's worth of data.

.. ipython:: python

   class CSVLineHandler(object):
        "Read data stored as lines in a text file."
        def __init__(self, filename):
            "Each instance of a Handler accesses a given file."
            self.file = open(filename)
            self.contents = self.file.readlines()
        def __call__(self, line_no):
            "Get a piece of data."
            return self.contents[line_no]


Make a Record of the Data
-------------------------

During data collection, we make a record of this file by calling
``insert_resource``. When you read "resource," you can think "file", but
other resources are also possible. Handlers can access URLs, URIs, or
even generate their results on the fly with no reference to external
information (e.g., synthetic testing data).

The arguments to ``insert_resource`` are

* A 'spec' labeling this data format so we know how to read it later --- we'll
  use ``'csv'``
* A filepath
* A dictionary of any additional keyword argument needed by `__init__`` above
  to locate and open the file. In our case, there are none, so we pass an empty
  dictionary ``{}``.

.. ipython:: python

   from databroker import Broker
   db = Broker.named('temp')  # builds an example db we can play with
   resource_id = db.reg.insert_resource('csv', 'example.csv', {})
   resource_id

Now, we create a record for each piece of data we'd like to retrieve. We
assign each one a unique ID, which we can use later to retrieve it.

The arguments here are the ``resource_id`` returned by ``insert_resource``
above, the unique ID we'll use to retrieve each piece of data, and finally the
arguments needed by ``__call__`` to read that data from the file. In our
case, ``__call__`` needs just one argument, ``line_no``.

.. ipython:: python

   db.reg.insert_datum(resource_id, 'some_id1', {'line_no': 1})
   db.reg.insert_datum(resource_id, 'some_id2', {'line_no': 2})
   db.reg.insert_datum(resource_id, 'some_id3', {'line_no': 3})
   db.reg.insert_datum(resource_id, 'some_id4', {'line_no': 4})
   db.reg.insert_datum(resource_id, 'some_id5', {'line_no': 5})

The Payoff: Retrieving Data Is Dead Simple
------------------------------------------

When we called ``insert_resource``, we recorded the nickname ``'csv'``. To read
that data, we have to associate ``'csv'`` and our Handler, ``CSVLineHandler``,
like so.

.. ipython:: python

   db.reg.register_handler('csv', CSVLineHandler)

Finally, we are ready to retrieve that data. All we need is the unique ID.

.. ipython:: python

   db.reg.retrieve('some_id2')

The Registry now knows to use the ``CSVLineHandler`` class, it knows to
instantiate it with ``example.csv``, and it knows to call it with the argument
``line_no=2``.

.. ipython:: python
   :suppress:

   !rm example.csv

Example 2: Retrieving Datasets from an HDF5 File
================================================

Suppose three 5x5 images are stored as Datasets in an one HDF5 file, named
``'A'``, ``'B'``, ``'C'``. We will treat the file as a "resource" and each HDF5
Dataset as a "datum".

.. ipython:: python
   :suppress:

   import h5py
   import numpy as np
   f = h5py.File('example.h5')
   for key in list('ABC'):
       f.create_dataset(key, data=np.random.randint(0, 10, (5, 5)))
   f.close()

Write a Handler
---------------

.. ipython:: python

   import h5py

   class HDF5DatasetHandler(object):
       def __init__(self, filename):
           self.file = h5py.File(filename)
       def __call__(self, key):
           return self.file[key][()]

Make a Record of the Data
-------------------------

.. ipython:: python

   resource_id = db.reg.insert_resource('hdf5-by-dataset', 'example.h5', {})
   db.reg.insert_datum(resource_id, 'some_id10', {'key': 'A'})
   db.reg.insert_datum(resource_id, 'some_id11', {'key': 'B'})
   db.reg.insert_datum(resource_id, 'some_id12', {'key': 'C'})

Retrieve the Data
-----------------

.. ipython:: python

   db.reg.register_handler('hdf5-by-dataset', HDF5DatasetHandler)
   db.reg.retrieve('some_id11')

.. ipython:: python
   :suppress:

   !rm example.h5

Example 3: Retrieving Portions of Datasets from an HDF5 File
============================================================

Suppose several 5x5 images are stored as a single Nx5x5 Dataset in an HDF5
file. The Dataset is named ``'my-dataset-name``. We will make the file a
"resource" and each 5x5 frame a "datum".

.. ipython:: python
   :suppress:

   import h5py
   import numpy as np
   f = h5py.File('example.h5')
   f.create_dataset('my-dataset-name',
                    data=np.random.randint(0, 10, (5, 5, 3)))
   f.close()

Write a Handler
---------------

.. ipython:: python

   import h5py

   class HDF5DatasetSliceHandler(object):
       def __init__(self, filename, dataset_name):
           f = h5py.File(filename)
           self.data = f[dataset_name][()]
       def __call__(self, frame_no):
           return self.data[frame_no, :, :]

Make a Record of the Data
-------------------------

Each 5x5 frame get a separate record.

Notice that, in this example, ``__init__`` requires more than just the
filename. Additional arguments, in this case ``dataset_name``, are passed
in a dictionary.

.. ipython:: python

   resource_id = db.reg.insert_resource('hdf5-slice-single-dataset',
                                        'example.h5',
                                        {'dataset_name': 'my-dataset-name'})
   db.reg.insert_datum(resource_id, 'some_id20', {'frame_no': 0})
   db.reg.insert_datum(resource_id, 'some_id21', {'frame_no': 1})
   db.reg.insert_datum(resource_id, 'some_id22', {'frame_no': 2})

Retrieve the Data
-----------------

.. ipython:: python

   db.reg.register_handler('hdf5-slice-single-dataset', HDF5DatasetSliceHandler)
   db.reg.retrieve('some_id21')

.. ipython:: python
   :suppress:

   !rm example.h5

Example 4: Retrieving the Moon Phase
====================================

This example illustrates the general power of this design, beyond reading
simple files. Any "resource," include a web-based data source, can be
accessed with a Handler.

Write a Handler
---------------

When we retrieve data from this handler, it builds a URL that requests the
weather forcast (or historical record) from the web service forecast.io.
From this data it extracts the phase of the moon at a given time.

.. ipython:: python

   import json
   import os
   import requests
   url = "https://api.forecast.io/forecast/{api_key}/{lat},{long},{time}"
   api_key = os.environ.get('FORECAST_IO_API_KEY', None)
   class MoonPhaseHandler(object):
       def __init__(self, _):
           "This handler does not require a filename."
           pass
       def __call__(self, time):
           if api_key is None:
                return 'Waxing'
           text = requests.get(url.format(api_key=api_key, lat=0, long=0,
                                          time=int(time))).text
           data = json.loads(text)
           return data['daily']['data'][0]['moonPhase']

Make a Record of the Data
-------------------------

Notably, in this case, we are making a record of data that we haven't seen yet.
The data itself will only be obtained for the first time when it retrieved.

.. ipython:: python

   resource_id = db.reg.insert_resource('moon', None, {})

Let's register a piece of data giving today's moon phase.

.. ipython:: python

   import time
   now = time.time()
   db.reg.insert_datum(resource_id, 'some_id31', {'time': now})

Retrieve Data
-------------


.. ipython:: python

   db.reg.register_handler('moon', MoonPhaseHandler)
   db.reg.retrieve('some_id31')
