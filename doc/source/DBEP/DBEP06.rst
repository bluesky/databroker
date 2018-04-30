===================
 DBEP 7 : Document mutation
===================


.. contents::
   :local:

Status
======

**Discussion**


Branches and Pull requests
==========================


Abstract
========
The motivation of this DBEP is the handling of document mutation.
There is a need for post-acquisition modification of metadata.





Proposed Implementations
====================
There are three ways that have been discussed for handling this.
Which one should be used has not been decided on as of now.


1. Copy the original data into a backup data store. Create a new start document
   with a new uid with the updated metadata. Lutz didn't like this because this
   isn't backwards compatible with his previous measurements.
   * Pros: Data is never mutated, less risk for data corruption
   * Cons: This is not very backwards compatible for beamline scientists. I.e.
     retrieval of data by uid must be updated with the newest uid used.
2. Modify the existing start document, and save the changes made in a separate
   changes database.
   * Pros : The user is unaffected by the changes
   * Cons : Once the change is made, it is a little difficult to view the
     documents without the change.
3. Create a separate database (mongo collection, sqlite database, spreadsheet,
   text file...) that refers to the Run Start UID and includes whatever extra
   information is desired. A convenience layer could be developed that joins
   this information together on the way out.
   * Pros : This could likely be easily turned on or off, since the addition of
     the changes is performed upon request.
   * Cons : The changes must be made as the documents are accessed. A new
     function must be supported/debugged for accessing data.


Resources/similar work
======================


Backwards Compatibility
=======================
