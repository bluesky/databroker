__author__ = 'arkilic'

from mongoengine import connect
from conf import database, host, port
from fileStore.database.file_base import FileBase
from fileStore.database.file_attributes import FileAttributes
from fileStore.database.file_event_link import FileEventLink
#TODO: Add documentation


def save_file_base(spec, file_path, custom=None):
    """
    Parameters
    ----------

    spec: str
        File spec used to primarily parse the contents into analysis environment

    file_path: str
        Url to the physical location of the file

    custom: dict
        custom name/value container in case additional info save is required

    """
    connect(db=database, host=host, port=port)

    file_base_object = FileBase(spec=spec, file_path=file_path, custom=custom)

    file_base_object.save(validate=True, write_concern={"w": 1})

    return file_base_object


def save_file_attributes(file_base, shape, dtype, **kwargs):
    """

    file_base:


    kwargs
    ------


    """

    connect(db=database, host=host, port=port)

    file_attributes = FileAttributes(file_base=file_base.id, shape=shape, dtype=dtype)

    file_attributes.total_bytes = kwargs.pop('total_bytes', None)
    file_attributes.hashed_data = kwargs.pop('hashed_data', None)
    file_attributes.last_access = kwargs.pop('last_access', None)
    file_attributes.datetime_last_access = kwargs.pop('datetime_last_access', None)
    file_attributes.in_use = kwargs.pop('in_use', None)
    file_attributes.custom_attributes = kwargs.pop('custom_attributes', None)

    if kwargs:
        raise AttributeError(kwargs.keys() + '  field(s) are not among attribute keys. Use custom attributes'
                                             ' dict for saving it')
    file_attributes.save(validate=True, write_concern={"w": 1})

    return file_attributes


def save_file_event_link(file_base, event_id, link_parameters=None):
    """

    Parameters
    ----------

    file_base: fileStore.database.file_base.FileBase
        FileBase object

    event_id:

    link_parameters:

    """

    connect(db=database, host=host, port=port)

    file_event_link = FileEventLink(file_base=file_base.id, event_id=event_id, link_parameters=link_parameters)
    file_event_link.save(validate=True, write_concern={"w": 1})

    return file_event_link


def find_file_base(**kwargs):

    query_dict = dict()

    connect(db=database, host=host, port=port)

    try:
        query_dict['spec'] = kwargs.pop['spec']
    except:
        pass

    try:
        query_dict['file_path'] = kwargs.pop['file_path']
    except:
        pass

    if kwargs:
        raise AttributeError('Search on ', kwargs.keys(), ' is not provided')

    file_base_objects = FileBase.objects(__raw__=query_dict).order_by('-_id')

    return file_base_objects


def find_last():
    """Searches for the last file_base created

    Returns
    --------

    FileBase object
    #I know i am violating numpy docs like a turkish in open buffet but have not looked into how to use it yet!

    """

    connect(db=database, host=host, port=port)

    return FileBase.objects.order_by('-_id')[0:1][0]


def find():

    raise NotImplementedError('Commands coming soon')
