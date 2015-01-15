__author__ = 'arkilic'


from mongoengine import Document, ReferenceField, DictField, DENY, StringField
from fileStore.database import file_base


class FileEventLink(Document):
    """
    Parameters
    ----------

    file_id:

    event_id:

    link_parameters:

    """
    file_base = ReferenceField(file_base.FileBase, reverse_delete_rule=DENY, required=True)
    event_id = StringField(required=True)
    link_parameters = DictField(required=False)
#TODO: add indexing
#TODO: add documentation