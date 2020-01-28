# This module is experimental. It is not documented or covered by automated
# tests. It may change in a backward-incompatible way in a future release of
# bluesky.
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

from datetime import datetime
import pytz
import uuid

import numpy as np

import requests
import pandas as pd

from ..utils import ALL


class ArchiverEventSource(object):

    def __init__(self, config):
        """
        Shim class to turn the EPICS Archiver Appliance into EventSource
        """

        if all(k in config for k in ['name', 'url', 'timezone']):
            pass
        else:
            raise TypeError("config {} does not include one of required"
                            " keys (name, url, timezone).".format(config))

        self._name = config['name']

        url = config['url']
        if not url.endswith('/'):
            url += '/'
        self.url = url
        self.archiver_addr = self.url + "retrieval/data/getData.json"

        timezone = config['timezone']
        self.tz = pytz.timezone(timezone)

        if 'pvs' in config:
            pvs = config['pvs']
        else:
            pvs = {}
        self.pvs = pvs

        self._descriptors = {}

    @property
    def name(self):
        return self._name

    def insert(self, name, doc):
        """
        Not supported, data archiving is managed via the EPICS Archiver
        """
        msg = "Not supported, data archiving is managed via"
        msg += " the EPICS Archiver Appliance Toolkit"
        raise NotImplementedError("Not supported, data archiving is "
                                  "managed via the EPICS Archiver "
                                  "Appliance Toolkit")

    def stream_names_given_header(self, header):
        # We actually don't use the header in this case.
        """
        Return a list of user-defined PV names

        Parameters
        ----------
        header : Header
            not used. stream_names are generated from pvs

        Returns
        -------
        list : list of user-defined PV names
        """

        return list(self.pvs)

    def fields_given_header(self, header):
        # We actually don't use the header in this case.
        """
        Return an empty set since fields are not used

        Parameters
        ----------
        header : Header
            not used.

        Returns
        -------
        set : empty set
        """

        return set()

    def descriptors_given_header(self, header):
        """
        Return PV descriptors for given Header

        Parameters
        ----------
        header : Header

        Returns
        -------
        list : list of PV descriptors
        """
        run_start_uid = header['start']['uid']

        try:
            return list(self._descriptors[run_start_uid])
        except KeyError:
            # Mock up descriptors and cache them so that the ephemeral uid is
            # stable for the duration of this process.
            descs = []
            since = header['start']['time'],
            until = header['stop']['time']
            for name, pv in six.iteritems(self.pvs):
                data_keys = {name: {'source': pv,
                                    'dtype': 'number',
                                    'shape': []}}
                _from = _munge_time(since[0], self.tz)
                # because since is a tuple^
                _to = _munge_time(until, self.tz)
                params = {'pv': pv, 'from': _from, 'to': _to}
                desc = {'name': name,
                        'time': header['start']['time'],
                        'uid': 'empheral-' + str(uuid.uuid4()),
                        'data_keys': data_keys,
                        'run_start': header['start']['uid'],
                        'external_query': params,
                        'external_url': self.url}
                descs.append(desc)
            self._descriptors[run_start_uid] = descs
            return list(self._descriptors[run_start_uid])

    def docs_given_header(self, header, stream_name=ALL, fields=None):
        """
        Return documents for given Header

        Parameters
        ----------
        header : Header
            The header to fetch the events for
        stream_name : string
            user-defined PV name
        fields : list, not used
            names of interest are defined via user-defined PVs

        Yields
        ------
        str : name
            The name of the document being yielded
        doc : Document
            The data payload
        """

        desc_uids = {}

        since, until = header['start']['time'], header['stop']['time']
        _from = _munge_time(since, self.tz)
        _to = _munge_time(until, self.tz)

        yield 'start', header['start']

        for d in self.descriptors_given_header(header):

            if d['name'] == stream_name:

                yield 'descriptor', d

                # Stash the desc uids in a local var
                # so we can use them in events.
                name = list(d['data_keys'].keys())[0]
                pv = list(d['data_keys'].values())[0]['source']
                desc_uids[pv] = d['uid']

                params = {'pv': pv, 'from': _from, 'to': _to}

                req = requests.get(self.archiver_addr, params=params,
                                   stream=True)
                req.raise_for_status()
                raw, = req.json()

                ts = [x['secs'] for x in raw['data']]
                data = [x['val'] for x in raw['data']]

                for seq_num, (v, t) in enumerate(zip(data, ts), start=1):
                    doc = {'data': {name: v},
                           'timestamps': {name: t},
                           'time': t,
                           'uid': 'ephemeral-' + str(uuid.uuid4()),
                           'seq_num': seq_num,
                           'descriptor': desc_uids[pv]}
                    yield 'event', doc

            else:
                continue

        yield 'stop', header['stop']

    def _table_given_times(self, pv, since, until):

        _from = _munge_time(since, self.tz)
        _to = _munge_time(until, self.tz)

        params = {'pv': pv, 'from': _from, 'to': _to}

        req = requests.get(self.archiver_addr, params=params, stream=True)
        req.raise_for_status()
        raw, = req.json()

        secs = [x['secs'] for x in raw['data']]
        nanos = [x['nanos'] for x in raw['data']]
        data = [x['val'] for x in raw['data']]

        asecs = np.asarray(secs)
        ananos = np.asarray(nanos)
        times = asecs*1.0e+3 + ananos*1.0e-6

        datetimes = pd.to_datetime(times, unit='ms', utc=True)

        df = pd.DataFrame()
        df['time'] = datetimes.tz_convert(tz=self.tz)
        df['data'] = data

        return df

    def tables_given_times(self, since, until):

        """
        Make PV tables (pandas.DataFrame) for given time interval

        Parameters
        ----------
        since : timestamp
            beginning of the time interval
        until : timestamp
            end of the time interval

        Returns
        -------
        table : dictionary of the pv pandas.DataFrames
        """

        dfs = {}
        for key in self.pvs.keys():
            dfs[key] = self._table_given_times(self.pvs[key], since, until)

        return dfs

    def table_given_header(self, header, stream_name=ALL,
                           fields=None, convert_times=True,
                           timezone=None, localize_times=True):

        """
        Make PV table (pandas.DataFrame) from given Header

        Parameters
        ----------
        header : Header
            The header to fetch the table for
        stream_name : string,
            PV stream_name (e.g., archiver_PV1)
        fields : list, not used
            names of interest are defined via user-defined PVs
        convert_times : bool, optional
            Whether to convert times from float (seconds since 1970) to
            numpy datetime64, using pandas. True by default, returns naive
            datetime64 objects in UTC
        timezone : str, optional
            e.g., 'US/Eastern'
        localize_times : bool, optional
            If the times should be localized to the 'local' time zone.  If
            True (the default) the time stamps are converted to
            the localtime zone.

        Returns
        -------
        tables : dictionary of the pv pandas.DataFrames
        """

        if timezone is None:
            timezone = self.tz.zone

        since, until = header['start']['time'], header['stop']['time']

        df = pd.DataFrame()

        if stream_name in self.stream_names_given_header(header):
            name = stream_name
            pv = self.pvs[name]
            df = self._table_given_times(pv, since, until)
            df.rename(columns={"time": "time", "data": stream_name},
                      inplace=True)
            new_index = np.arange(1, df.index.size + 1)
            df.index = new_index

        return df


def _munge_time(t, timezone):
    """Close your eyes and trust @arkilic

    Parameters
    ----------
    t : float
        POSIX (seconds since 1970)
    timezone : pytz object
        e.g. ``pytz.timezone('US/Eastern')``

    Return
    ------
    time
        as ISO-8601 format
    """
    t = datetime.fromtimestamp(t)
    return timezone.localize(t).replace(microsecond=0).isoformat()
