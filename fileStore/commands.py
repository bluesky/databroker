__author__ = 'arkilic'
from mongoengine import connect

from .conf import database, host, port
from .database.file_base import FileBase
from .database.file_attributes import FileAttributes
from .database.file_event_link import FileEventLink
from .retrieve import get_data as _get_data
# TODO: Add documentation


def save_file_base(spec, file_path, custom=None, collection_version=0):
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

    file_base_object = FileBase(spec=spec, file_path=file_path, custom=custom, collection_version=collection_version)

    file_base_object.save(validate=True, write_concern={"w": 1})

    return file_base_object


def save_file_attributes(file_base, shape, dtype, collection_version=0, **kwargs):
    """

    file_base:


    kwargs
    ------


    """

    connect(db=database, host=host, port=port)

    file_attributes = FileAttributes(file_base=file_base.id, shape=shape, dtype=dtype,
                                     collection_version=collection_version)

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


def save_file_event_link(file_base, event_id, link_parameters=None, collection_version=0):
    """

    Parameters
    ----------

    file_base: fileStore.database.file_base.FileBase
        FileBase object

    event_id: str
        metadataStore unique event identifier in string format

    link_parameters: dict
        custom dict required for appending name/value pairs as desired

    """

    connect(db=database, host=host, port=port)

    file_event_link = FileEventLink(file_base=file_base.id, event_id=event_id, link_parameters=link_parameters)
    file_event_link.save(validate=True, write_concern={"w": 1})

    return file_event_link


def find_file_base(**kwargs):

    query_dict = dict()
    print kwargs
    connect(db=database, host=host, port=port)

    try:
        query_dict['spec'] = kwargs.pop('spec')
    except:
        pass

    try:
        query_dict['file_path'] = kwargs.pop('file_path')
    except:
        pass

    file_base_objects = FileBase.objects(__raw__=query_dict).order_by('-_id')

    return file_base_objects


def find_file_event_link(file_base=None, event_id=None):

    query_dict = dict()

    connect(db=database, host=host, port=port)

    if file_base is not None:
        query_dict['file_base'] = file_base.id
    elif event_id is not None:
        query_dict['event_id'] = event_id
    else:
        raise AttributeError('Search parameters are invalid. file_base or event_id search is possible')

    return FileEventLink.objects(__raw__=query_dict)



def find_last():
    """Searches for the last file_base created

    Returns
    --------

    FileBase object
    #I know i am violating numpy docs like a turkish in open buffet but have not looked into how to use it yet, until
    #we decide what doc format to use, I will not bother, just write stuff we need for any format!

    """

    connect(db=database, host=host, port=port)

    result = FileBase.objects.order_by('-_id')[0:1][0]
    if result:
        return result
    else:
        return []


def find_file_attributes(file_base):
    """Return  file_attributes entry given a file_header object

    Parameters
    ----------
    file_base: fileStore.database.file_base.FileBase
        FileBase object

    """

    connect(db=database, host=host, port=port)

    return FileAttributes.objects(file_base=file_base.id)


def find(properties=True, **kwargs):
    file_attribute_objects = list()
    file_event_link_objects = list()
    file_base_objects = find_file_base(**kwargs)

    for file_base_object in file_base_objects:
        file_attribute_objects.append(find_file_attributes(file_base=file_base_object))
        file_event_link_objects.append(find_file_event_link(file_base=file_base_object))

    return file_base_objects, file_attribute_objects, file_event_link_objects