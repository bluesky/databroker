__author__ = 'arkilic'


from mongoengine import Document, ReferenceField, DictField, DENY, StringField, FloatField
from fileStore.database import file_base


class FileEventLink(Document):
    """Correlation lookup table between events and files. Primarily for dataBroker logic

    Parameters
    ----------

    file_id: fileStore.file_base.FileBase
        FileBase object required to create an event link. id field is used to obtain the foreignkey

    event_id: str
        metadataStore unqiue event identifier in string format.

    link_parameters: dict
        custom dictionary required for appending name/value pairs as desired
    """
    file_base = ReferenceField(file_base.FileBase, reverse_delete_rule=DENY, required=True)
    event_id = StringField(required=True)
    link_parameters = DictField(required=False)
    collection_version = FloatField(required=False, min_value=0)
    meta = {'indexes':['-_id', '-event_id', '-file_base']}