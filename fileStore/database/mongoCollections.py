__author__ = 'arkilic'


class FileBase(object):
    def __init__(self, file_id, spec, file_path, custom):
        """
        Stores the most basic information required to create a file hash that is generated via data collection code.
        This hash is then used in metadataStore event field to be

        :param file_id: File identifier, hash
        :param spec: Method to parse a file
        :param file_path: Physical location in file system
        :return: None
        """

        self.__file_id = file_id
        self.__spec = spec
        self.__file_path = file_path
        self.__custom = custom
        #TODO: Add data type verification
        self.bson = self.bsonify()

    def bsonify(self):
        """
        Compose a bson-like python dict to be inserted in fileStore database

        :return: bsonified FileStore base class attributes
        :rtype: dict
        """
        bson = dict()
        bson['file_id'] = self.__file_id
        bson['spec'] = self.__spec
        bson['file_path'] = self.__file_path
        return bson


class FilePublic(object):
    def __init__(self, shape, dtype, **kwargs):
        self.shape = shape
        self.dtype = dtype
        self.bson = self.bsonify()
        try:
            self.total_bytes = kwargs['total_bytes']
        except KeyError:
            self.total_bytes = 'N/A'

        try:
            self.hashed_data = kwargs['hashed_data']
        except KeyError:
            self.hashed_data = 'N/A'

        try:
            self.last_access = kwargs['last_access']
        except KeyError:
            self.last_access = 'N/A'

        try:
            self.in_use = kwargs['in_use']
        except KeyError:
            self.in_use = 'N/A'
        try:
            self.custom = kwargs['custom']
        except KeyError:
            self.custom = dict()
        self.bson = self.bsonify()

    def bsonify(self):
        """
        :return:
        """
        bson_dict = dict()
        bson_dict['shape'] = self.shape
        bson_dict['dtype'] = self.dtype
        bson_dict['total_bytes'] = self.total_bytes
        bson_dict['hashed_data'] = self.hashed_data
        bson_dict['last_access'] = self.last_access
        bson_dict['in_use'] = self.in_use
        return bson_dict


class EventList(object):
    def __init__(self, event_id, file_id, event_list_custom):
        """

        :param event_id:
        :param file_id:
        :param event_list_custom:
        :return:
        """
        self.event_id = event_id
        self.file_id = file_id
        self.event_list_custom = event_list_custom
        self.bson = self.bsonify()

    def bsonify(self):
        """

        :return:
        """
        bson_dict = dict()
        bson_dict['event_id'] = self.event_id
        bson_dict['file_id'] = self.file_id
        bson_dict['event_list_custom'] = self.event_list_custom
        return bson_dict