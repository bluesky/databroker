=============================
 Import from folder of files
=============================

.. contents::
   :local:



Status
======

DBEPs go through a number of phases in their lifetime:

- **Discussion**:


Branches and Pull requests
==========================

Abstract
========

Given a top-level directory that contains files we want to be able to
index and ingest the files into an in-place databoker.  This proposes
an interface to make it easier to write the minimum code to cope with
a new file layout or format.

Detailed description
====================

This is going to assume that DBEP04 is through.

The first thing we need is a way to sort out what files
belong together ::

  from typing import Generator, Tuple
  from pathlib import Path


  def file_grouper(path : Path) -> Generator[Tuple[Path]]:
      """Partition a file tree into runs

      Parameters
      ----------
      path : Path
          The directory to recursively search for files to ingest

      Yields
      ------
      file_type : str
         Name of the file type
      file_list : Tuple[Path]
         Each run is yielded as a tuple of Path objects.

      """
      ...

For homogeneous folders this is overkill, but this will let us write a
single-point-of-entry searcher to ingest mixed folders

The next thing we need as an interface for a class to handle the files
associated with a single run ::


Implementation
==============


Backward compatibility
======================

New functionality.


Alternatives
============

status-quo / nil
