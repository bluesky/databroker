from itertools import chain
# BEGIN LPy
try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest
# END LPy
import uuid
import time as ttime
import operator as op
from functools import reduce


def pivot_timeseries(events, pivot_keys, static_keys=None):
    """Pivot sequence of events with sequences as data to single sequence

    For example a sequence of 5 events with 100 frames of 2D data per datum
    will turn into a sequence of 500 events which each have a single frame.

    Any keys not in ``pivot_keys`` or ``static_keys`` will be dropped.

    This will return new event documents with a new Descriptor which is
    not in the database.


    Parameters
    ----------
    events : sequence
        A sequence of events all with the same Descriptor

    pivot_keys : sequence
        A list of keys from the events to pivot out

    static_keys : sequence, optional
        A list of keys from the original events to propagate
        to the new events

    Yields
    ------
    event : doc
        The pivoted document
    """
    if static_keys is None:
        static_keys = []
    if set(pivot_keys) & set(static_keys):
        raise RuntimeError("Pivot and static_keys overlap")

    events = iter(events)
    ev0 = next(events)
    orig_desc = ev0['descriptor']
    new_desc = {'uid': str(uuid.uuid4()),
                'data_keys': dict(),
                'run_start': orig_desc['run_start'],
                'time': ttime.time()}
    new_desc['data_keys']['_ind'] = {'shape': (),
                                     'dtype': 'number',
                                     'source': 'pivot'}
    for key in static_keys:
        new_desc['data_keys'][key] = orig_desc['data_keys'][key]
    for key in pivot_keys:
        orig_data_key = orig_desc['data_keys'][key]
        if orig_data_key['dtype'] != 'array':
            raise RuntimeError("trying to pivot on non-array data")

    pv_lens = [orig_desc['data_keys'][k]['shape'][0] for k in pivot_keys]
    if not all(pv_lens[0] == pl for pl in pv_lens):
        raise RuntimeError("not all pivot columns are the same length")

    seq_no = 0
    for ev in chain((ev0, ), events):
        inner_desc = dict(new_desc)
        inner_desc['data_keys'] = dict(inner_desc['data_keys'])
        for key in pivot_keys:
            orig_data_key = orig_desc['data_keys'][key]
            shape = orig_data_key['shape'][1:]
            dtype = 'array' if shape else 'number'
            inner_desc['data_keys'][key] = {'shape': shape,
                                            'dtype': dtype,
                                            'source': ev['uid']}

        data = ev['data']
        ts = ev['timestamps']
        time = ev['time']
        static_data = {k: data[k] for k in static_keys}
        static_ts = {k: ts[k] for k in chain(static_keys, pivot_keys)}

        pivot_data = zip(*[data[k] for k in pivot_keys])

        for _ind, _data in enumerate(pivot_data):
            new_ev = {'uid': str(uuid.uuid4()),
                      'time': time,
                      'descriptor': inner_desc,
                      'seq_no': seq_no,
                      }
            inner_data = dict(static_data)
            inner_data.update({k: v for k, v in zip(pivot_keys, _data)})
            inner_data['_ind'] = _ind
            inner_ts = dict(static_ts)
            inner_ts['_ind'] = ttime.time()
            new_ev['data'] = inner_data
            new_ev['timestamps'] = inner_ts
            yield new_ev
            seq_no += 1


def zip_events(*evs, **kwargs):
    """Zip together a collection of event streams

    All events in a single stream must have the same
    descriptor.

    All event streams must be the same length

    All event streams must have same run_start (for now)

    Parameters
    ----------
    *evs
        sequences that yield events

    lazy : bool, optional
        If False, listify event streams first for validation
        defaults to True
    """
    # BEGIN LPy
    # Once we drop LPy support the signature can be changed back to
    # def zip_events(*evs, lazy=True)
    # the code below is to work around limitations of the LPy syntax
    lazy = kwargs.pop('lazy', True)
    if len(kwargs):
        raise TypeError("Passed unknown key word arguments")
    # END LPy

    if not lazy:
        # need to listify
        evs = [list(ev) for ev in evs]
        lens = [len(ev) for ev in evs]
        # TODO check single descriptor invariant
        if not all(l == lens[0] for l in lens):
            raise RuntimeError("All event streams must be same len")

    # set up iterators and spy on the first event
    evs = [iter(ev) for ev in evs]
    first_events = [next(ev) for ev in evs]
    evs = [chain((ev0, ), ev) for ev0, ev in zip(first_events, evs)]
    # make new descriptor
    descs = [ev['descriptor'] for ev in first_events]
    if not all(d['run_start'] == descs[0]['run_start'] for d in descs):
        raise RuntimeError("Not all descriptors have same runstart")
    sum_keys = set(chain(*(d['data_keys'] for d in descs)))
    if len(sum_keys) != reduce(op.add, [len(d['data_keys']) for d in descs]):
        raise RuntimeError("event descriptors have overlapping keys")
    zip_desc = {'uid': str(uuid.uuid4()),
                'data_keys': dict(),
                'run_start': descs[0]['run_start'],
                'time': ttime.time()}
    for d in descs:
        zip_desc['data_keys'].update(d['data_keys'])

    for seq_no, events in enumerate(zip_longest(*evs)):
        if any(ev is None for ev in events):
            raise RuntimeError("not all streams same length")
        for ev, desc in zip(events, descs):
            if ev['descriptor']['uid'] != desc['uid']:
                raise RuntimeError("one of event streams changes descriptors")
        new_ev = {'uid': str(uuid.uuid4()),
                  'time': ttime.time(),
                  'descriptor': zip_desc,
                  'seq_no': seq_no,
                  'data': dict(),
                  'timestamps': dict()}
        for ev in events:
            new_ev['data'].update(ev['data'])
            new_ev['timestamps'].update(ev['timestamps'])

        yield new_ev


def reset_time(evs, field, source='timestamps'):
    """Reset the time field on the event.

    Promote a specific timestamp to be the time of the whole
    event.

    Parameters
    ----------
    evs : iterable
        Iterable of events to work on

    field : str
        The field to use for the value

    source : {'timestamps', 'data'}, optional
        Where to look for that field

    Yields
    ------
    events
    """
    for ev in evs:
        ev = dict(ev)
        ev['time'] = ev[source][field]
        yield ev
