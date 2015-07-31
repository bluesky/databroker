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
   - This may be trouble for some usage patterns where multiple
     resources point to same file
 - move files from one place to another
 - delete files
 - delete resources
 - verify data at both file system and Datum level

Implementation
==============

General Requirements
--------------------

 - implement Datum-level hashing
   - this should be a new collection which is keyed on DatumID and
     contains the hash (sha1 or md5) of the values
   - may contain additional statistics, proprieties about datum
     - shape, dtype, (min, max, mean, histogram ?)
     - may want to stats as separate transient DB
 - each file spec needs class/handler that will, given a resource,
   produce a list of all the files that are needed
 - implement resource < - > absolute path mapping collection
   - this is transient as it can always be re-generated
   - need a way to flag as 'alive' or not
 - implement hashing of files
 - maybe implement a chroot, as well as path into Resource
   - this is so that you can say ``change_root(resource_id, new_root)``
     and then the files along with the folder structure would be moved.
   - without doing this we could do something like
     ``change_root(resource_id, n_base, new_root)`` where n_base is
     how many layers of directory to strip off, but this requires
     knowing a fair amount about the actually paths involved in the
   - Could also do something like ``change_path(path_mutation_func,
     resource_id)`` where ``path_mutation_func`` is a str -> str
     mapping function which is general, but is not great in terms of
     keeping this a controlled process and puts a big burden on the
     user.
   - if there are multiple copies of the same file be able to control
     which version gets hit
     - this needs to be controllable based on which computer the compute
       is running on


API proposal
------------
New python API ::

   def copy_resource(resource_id, new_root, old_root=None):
       """Copy all the files of a resource

       Parameters
       ----------
       resource_id : uuid
           The unique id of the resource to work on

       new_root : str
           The path to the location in the filesystem to cop
	   the files into.  The full existing directory structure
	   will be replicated on top of the now root

       old_root : str, optional
           If there exists more than one copy already, select
	   which one to use

       """

   def move_resource(resource_id, old_root, new_root):
       """Move all files for a resource to a new location


       This is the same as copy then delete.  Because of the
       delete step users must be explicit about source path.

       Parameters
       ----------
       resource_id : uuid
           The unique id of the resource to work on

       old_root : str
           If there exists more than one copy already, select
	   which one to use

       new_root : str
           The path to the location in the filesystem to cop
	   the files into.  The full existing directory structure
	   will be replicated on top of the now root

       """

   def remove_resource(resource_id, old_root, force_last=False):
       """Delete all files associated with a resource

       Parameters
       ----------
       resource_id : uuid
           The unique id of the resource to work on

       old_root : str
           Which set of files to delete

       force_last : bool, optional
           If False, will raise RuntimeError rather than
	   delete the last copy of the files.


       """

New DB schema::


    class Resource(Document):
        """

        Parameters
        ----------

        spec : str
            spec used to determine what handler to use to open this
            resource.

        resource_path : str
            Url to the physical location of the resource

        resource_kwargs : dict
            name/value pairs of additional kwargs to be
            passed to the handler to open this resource.

        """

        spec = StringField(required=True, unique=False)
	root = StringField(required=True, unique=False)
        path = StringField(required=True, unique=False)
        kwargs = DictField(required=False)
        uid = StringField(required=True, unique=True)

        meta = {'indexes': ['-_id', 'resource_root'], 'db_alias': ALIAS}


    class File(Document):
        """
        This is 'semi-transient', everything in here can be rebuilt
        if needed from Resource, Datum, and their helper code, but
	the hash can be used for validation
        """
        resource_uid = StringField(required=True, unique=False)
        root = StringField(required=True, unique=False)

        uid = StringField(required=True, unique=True)
        abs_path = StringField(required=True, unique=True)
        sha1_hash = StringField(required=True)
	size = FloatField(required=True)
        exists = Bool(required=True)


    class DatumStats(DynamicDocument):
        datum_uid = StringField(required=True, unique=True)
	sha1_hash = StringField(required=True)
	shape = ListField(field=IntField())

    class CommandJournal(Document):
        command = StringField(required=True)
	args =  ListField()
	kwargs = DictField()
	success = Bool(required=True)


In a departure from our standard design protocol let File have the
'exists' field be updated.  Or have a collection which is just a
(resource_uid, root) create/delete journal.  Another option is to allow
``remove`` to delete entries from `File` collection.


Backward compatibility
======================

This will require a DB migration and breaks all of the AD instances that
insert into FS.

Alternatives
============

None yet
