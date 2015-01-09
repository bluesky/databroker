# ######################################################################
# Copyright (c) 2015, Brookhaven Science Associates, Brookhaven        #
# National Laboratory. All rights reserved.                            #
#                                                                      #
# Redistribution and use in source and binary forms, with or without   #
# modification, are permitted provided that the following conditions   #
# are met:                                                             #
#                                                                      #
# * Redistributions of source code must retain the above copyright     #
#   notice, this list of conditions and the following disclaimer.      #
#                                                                      #
# * Redistributions in binary form must reproduce the above copyright  #
#   notice this list of conditions and the following disclaimer in     #
#   the documentation and/or other materials provided with the         #
#   distribution.                                                      #
#                                                                      #
# * Neither the name of the Brookhaven Science Associates, Brookhaven  #
#   National Laboratory nor the names of its contributors may be used  #
#   to endorse or promote products derived from this software without  #
#   specific prior written permission.                                 #
#                                                                      #
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS  #
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT    #
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS    #
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE       #
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,           #
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES   #
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR   #
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)   #
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,  #
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OTHERWISE) ARISING   #
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE   #
# POSSIBILITY OF SUCH DAMAGE.                                          #
########################################################################
from __future__ import (absolute_import, division, print_function)

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
        bson['custom'] = dict(self.__custom)
        return bson


class FileAttributes(object):
    def __init__(self, shape, dtype, **kwargs):
        self.shape = shape
        self.dtype = dtype
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

    def bsonify(self):
        """

        :return:
        """
        bson_dict = dict()
        bson_dict['event_id'] = self.event_id
        bson_dict['file_id'] = self.file_id
        bson_dict['event_list_custom'] = self.event_list_custom
        return bson_dict