import mongoengine


class BrokerStruct(object):
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
                attr = BrokerStruct(attr)
            setattr(self, field, attr)

    def __repr__(self):
        return "<BrokerStruct {0}>".format(self._name)
