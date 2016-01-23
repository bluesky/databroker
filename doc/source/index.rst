.. NSLS-II arch documentation master file, created by
   sphinx-quickstart on Sun Jan 18 10:00:09 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Data Broker
***********

The databroker Python package provdies a simple, user-friendly interface
for retireving stored data and metadata. It retrieves data into memory as
built-in Python data types and numpy arrays.

Both simple and complex searches are supported. Convenience fucntions
provide a simple way to handle tabular data and image sequences.

Examples of Basic Usage
-----------------------

If they are not already imported, import ``DataBroker`` and its convenience
fucntions. (This step may already be done as part of an
`IPython profile <http://nsls-ii.github.io/beamline-configuration.html#ipython-profiles>`_.)

.. code-block:: python

    from databroker import DataBroker as db, get_table, get_images, get_events

Example 1: Load data in a table.
================================

.. code-block:: python

    last_run = db[-1]
    table = get_table(last_run)

To exclude non-scalar data* like images, which take time to load and are
usually useful to view in tabular form, use the ``fill`` keyword like so:

.. code-block:: python

    table = get_table(last_run, fill=False)

\*To be precise, ``fill=False`` excludes *externally stored* data, which
almost always means larges files and array data.

Example 2: Load select fields of data.
======================================

For faster loading, restrict the table to include only certain columns.

.. code-block:: python

    last_run = db[-1]
    table = get_table(last_run, ['temperature', 'motor_position'])

Example 3: Load images.
=======================

.. code-block:: python

    last_run = db[-1]
    images = get_images(last_run, 'ccd_image')

To plot the first image using matplotlib:

.. code-block:: python

    import matplotlib.pyplot as plt
    img = images[0]
    plt.imshow(img)

See :doc:`searching` and :doc:`fetching` for details.

.. toctree::
   :hidden:

   searching
   fetching
   whats_new
