.. NSLS-II arch documentation master file, created by
   sphinx-quickstart on Sun Jan 18 10:00:09 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

DataBroker and DataMuxer Documentation
======================================

Data Broker
-----------

Data Broker is a Python package that provdies a simple, user-friendly interface
for retireving stored data and metadata. It retrieves data into memory as
built-in Python data types and numpy arrays.

Both simple and complex searches are supported. Convenience fucntions
provide a simple way to handle tabular data and image sequences.
  
See the :ref:`api` for details.

Data Muxer
----------

Data Muxer is a tool for interleaving ("muxing") separate streams of data
based on time. For example, readings taking asynchronously must be aligned --
that is, assigned common bins in time -- before they can be plotted against
each other.

See the :ref:`api` for details.

.. toctree::
   :maxdepth: 1

   api
   whats_new
