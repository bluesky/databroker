__author__ = 'arkilic'

from mongoengine import DictField, Document


class BeamlineConfig(Document):
    """

    """
    config_params = DictField(required=False, unique=False)
    meta = {'indexes': ['-_id']}
