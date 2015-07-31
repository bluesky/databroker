==============
 Mobile files
==============

.. contents::
   :local:


Status
======

FEPs go through a number of phases in their lifetime:

- **Discussion**: The FEP is being actively discussed.

Branches and Pull requests
==========================

None yet

Abstract
========

This FEP adds the ability for filestore to copy / move files around
the file system and keep track of those changes.

Detailed description
====================

This FEP will provide API to

 - database to keep track of the full history of file locations
 - make a copy of all data from a resource from one location in the file
   system to another and update all relevant entries
   - This may be trouble for some usage patterns where multiple resources point to
     same file
 - move files from one place to another
 - delete files
 - delete resources
 - verify data at both file system and Datum level

Implementation
==============

This section lists the major steps required to implement the FEP.
Where possible, it should be noted where one step is dependent on
another, and which steps may be optionally omitted.  Where it makes
sense, each step should include a link related pull requests as the
implementation progresses.

Backward compatibility
======================

This section describes the ways in which the FEP breaks backward incompatibility.

Alternatives
============

If there were any alternative solutions to solving the same problem,
they should be discussed here, along with a justification for the
chosen approach.
