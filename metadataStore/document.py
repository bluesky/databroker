import mongoengine
from datetime import datetime


class Document(object):
    """
    Copy the data out of a mongoengine.Document, including nested Documents,
    but do not copy any of the mongo-specific methods or attributes.
    """
    def __init__(self, mongo_document):
        """
        Parameters
        ----------
        mongo_document : mongoengine.Document
        """
        self._name = mongo_document.__class__.__name__
        fields = mongo_document._fields
        for field in fields:
            attr = getattr(mongo_document, field)
            if isinstance(attr, mongoengine.Document):
                attr = Document(attr)
            setattr(self, field, attr)
            # For debugging, add a human-friendly time_as_datetime attribute.
            if hasattr(self, 'time'):
                self.time_as_datetime = datetime.fromtimestamp(self.time)


    def __repr__(self):
        return "<{0} Document>".format(self._name)
