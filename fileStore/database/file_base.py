__author__ = 'arkilic'


from mongoengine import Document, StringField, DictField


class FileBase(Document):
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

    spec = StringField(max_length=10, required=True, unique=False)
    file_path = StringField(max_length=100, required=True, unique=False)
    custom = DictField(required=False)
    meta = {'indexes': ['-file_path', '-_id']}

#TODO: add indexing