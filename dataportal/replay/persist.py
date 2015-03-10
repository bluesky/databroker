# ######################################################################
# Copyright (c) 2014, Brookhaven Science Associates, Brookhaven        #
# National Laboratory. All rights reserved.                            #
#                                                                      #
# BSD 3-Clause                                                         #
# ######################################################################

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

import sqlite3
import json


class History(object):
    """
    A helper class to make persisting configuration data easy.

    This class works by providing a this wrapper over a sqlite
    data base which has a column for the key, the number of times
    the key has been seen, and the data stored as a json-encoded blob.

    """
    def __init__(self, fname):
        self._conn = sqlite3.connect(fname)
        if not self._has_tables():
            self._create_tables()

    def get(self, key, num_back=0):
        """
        Retrieve a past state of the data payload associated with `key`,
        by default the most recent state.  Previous states can be accessed
        via the `num_back` kwarg which will retrieve the nth back entry (so
        `num_back=0` get the latest, `num_back=5` gets the fifth most recent.

        Parameters
        ----------
        key : str
            The key to look up data by

        num_back : int, optional
            Number back from the latest entry to retrieve.

        Returns
        -------
        data_blob : object
            Data payload
        """
        if num_back < 0:
            raise ValueError("num_back must be nonnegative")

        res = self.conn.execute()
        return json.loads(res)

    def put(self, key, data):
        """
        Store a data blob into the database

        Parameters
        ----------
        key : str
            The key to look up data by later

        data : object
            The data to be stored.  Can be any object that
            json can serialize.
        """
        data_str = json.dumps(data)
        self.conn.execute('', key, data_str)

    def trim(self, N=1):
        """
        Trim the databaase to keep at most N entries for
        all keys.

        Parameters
        ----------
        N : int, optional
            The number of entries to keep.  N < 1 is treated as 1
        """
        if N < 1:
            N = 1
        self.conn.execute('', N)

    def _create_tables(self):
        """
        Create the required tables for data storage
        """
        pass

    def _has_tables(self):
        """
        Determine of the opened file has the needed tables
        """
        return True
