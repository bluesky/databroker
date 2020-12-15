==============
 Mobile files
==============

.. contents::
   :local:


Status
======

FEPs go through a number of phases in their lifetime:

- **Partially Implemented**: The FEP is being actively discussed, a sub set has been implemented.

Branches and Pull requests
==========================

 - https://github.com/NSLS-II/filestore/pull/58

Abstract
========

This FEP adds the ability for filestore to copy / move files around
the file system and keep track of those changes.

Detailed description
====================

This FEP will provide API to

 - database to keep track of the full history of file locations *implemented*
 - make a copy of all data from a resource from one location in the file
   system to another and update all relevant entries *implemented*

   - This may be trouble for some usage patterns where multiple
     resources point to same file

 - move files from one place to another *implemented*
 - delete files *implemented*
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
   produce a list of all the files that are needed *partial, need to flesh out handlers*
 - implement resource < - > absolute path mapping collection

   - this is transient as it can always be re-generated
   - need a way to flag as 'alive' or not

 - implement hashing of files
 - maybe implement a chroot, as well as path into Resource *implemented, but not as described*

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

Currently Implemented
*********************

Limited API ::

  def change_root(resource, new_root, remove_origin=True, verify=False):
      '''Change the root directory of a given resource

      The registered handler must have a `get_file_list` method and the
      process running this method must have read/write access to both the
      source and destination file systems.


       Parameters
       ----------
       resource_or_uid : Document or str
           The resource to move the files of

       new_root : str
           The new 'root' to copy the files into

       remove_origin : bool, optional (True)
           If the source files should be removed

       verify : bool, optional (False)
           Verify that the move happened correctly.  This currently
           is not implemented and will raise if ``verify == True``.
      '''

   def shift_root(self, resource_or_uid, shift):
       '''Shift directory levels between root and resource_path

       This is useful because the root can be change via `change_root`.

       Parameters
       ----------
       resource_or_uid : Document or str
           The resource to change the root/resource_path allocation
           of absolute path.

       shift : int
           The amount to shift the split.  Positive numbers move more
           levels into the root and negative values move levels into
           the resource_path

       '''

    def insert_resource(self, spec, resource_path, resource_kwargs, root=''):



additional public API *draft*::

   def get_resources_by_root(root, partial=False):
       pass


   def get_resources_by_path(path, partial=False):
       pass


   def get_resources_by_spec(spec):
       pass


   def get_resource_by_uid(uid):
       pass


extended schema ::

  resource_update = {
      resource: uid,
      old: original_resource_doc,
      new: updated_serouce_doc,
      time: timestamp (posix time),
      cmd: str, the command that generated the insertion
      cmd_kwargs: dict, the inputs to cmd
      }

  resource = {
       spec: str,
       root: str,
       resource_path: str,
       resource_kwargs: dict,
       uid: str
       }

Full proposal
*************

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

   def insert_resource(spec, resource_root, resource_path, resource_kwargs=None):
       """
       Parameters
       ----------

       spec : str
           spec used to determine what handler to use to open this
           resource.

       resource_path, resource_root : str or None
           Url to the physical location of this resource

       resource_kwargs : dict, optional
           resource_kwargs name/value pairs of additional kwargs to be
           passed to the handler to open this resource.

       """

   def retrieve(eid, root_preference=None)
       """
       Given a resource identifier return the data.

       The root_preference allows control over which copy
       of the data is used if there is more than one available.

       Parameters
       ----------
       eid : str
           The resource ID (as stored in MDS)

       root_preference : list, optional
           A list of preferred root locations to pull data from in
	   descending order.

	   If None, fall back to configurable default.

       Returns
       -------
       data : ndarray
           The requested data as a numpy array
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
        path = StringField(required=True, unique=False)
        kwargs = DictField(required=False)
        uid = StringField(required=True, unique=True)

        meta = {'indexes': ['-_id', 'resource_root'], 'db_alias': ALIAS}


    class ResourceRoots(DynamicDocument):
        """
	Many to one mapping between Resource documents and chroot paths.

	The idea is that the absolute path of a file contains two
	parts, the root, which is set by details of how the file
	system is mounted, and the relative path which is set by some
	sort of semantics.  For example in the path ::

	    /mnt/DATA/2015/05/06/my_data.h5

	``/mnt/DATA/`` is the root and ``2015/05/06/my_data.h5`` is
	the relative path.

	In the case of a URL this would be ::

	  http://data.nsls-ii.bnl.gov/xf11id/2015/05/06/my_data.h5

	the root would be ``http://data.nsls-ii.bnl.gov/`` and the
	relative path would be ``xf11id/2015/05/06/my_data.h5``

	Parameters
	----------
	root : str
	    The chroot of the resource.

	resource_uid : str
	    The uid of the resource this is associated with

	"""
       	root = StringField(required=True, unique=False)
	resource_uid = StringField(required=True, unique=False)


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
