# ######################################################################
# Copyright (c) 2014, Brookhaven Science Associates, Brookhaven        #
# National Laboratory. All rights reserved.                            #
#                                                                      #
# BSD 3-Clause                                                         #
# ######################################################################

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
import logging
import hashlib
import sqlite3
import json


logger = logging.getLogger(__name__)


TABLE_NAME = 'ReplayConfiguration_1_0'
CREATION_QUERY = """
CREATE TABLE {0} (
        run_id CHAR(40),
        N INT,
        blob BLOB)""".format(TABLE_NAME)
INSERTION_QUERY = """
INSERT INTO {0}
(run_id, N, blob)
VALUES
(?,
1 + (SELECT COALESCE(MAX(N), 0)
     FROM {0}
     WHERE run_id=?),
?)""".format(TABLE_NAME)
SELECTION_QUERY = """
SELECT blob
FROM {0}
WHERE run_id=? ORDER BY N DESC LIMIT ?""".format(TABLE_NAME)
SHOW_TABLES_QUERY = """SELECT name FROM sqlite_master WHERE type='table'"""


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
            logger.debug("Created a fresh replay configuration table in %s,",
                         fname)
            self._create_tables()
        else:
            logger.debug("Found a replay configuration table in %s", fname)

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

        key = hashlib.sha1(str(key).encode('utf-8')).hexdigest()
        res = self._conn.execute(SELECTION_QUERY, (key, 1 + num_back))
        blob, = res.fetchall()[-1]
        return json.loads(blob)

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
        key = hashlib.sha1(str(key).encode('utf-8')).hexdigest()
        data_str = json.dumps(data)
        self._conn.execute(INSERTION_QUERY, (key, key, data_str))  # yes, twice
        self._conn.commit()

    def trim(self, N=1):
        """
        Trim the databaase to keep at most N entries for
        all keys.

        Parameters
        ----------
        N : int, optional
            The number of entries to keep.  N < 1 is treated as 1
        """
        raise NotImplementedError("TODO")

    def _create_tables(self):
        """
        Create the required tables for data storage
        """
        self._conn.execute(CREATION_QUERY)

    def _has_tables(self):
        """
        Determine of the opened file has the needed tables
        """
        res = self._conn.execute(SHOW_TABLES_QUERY)
        tables = [t[0] for t in res.fetchall()]
        return TABLE_NAME in tables
