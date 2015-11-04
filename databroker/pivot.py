from itertools import chain
import uuid
import time as ttime

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
        static_ts = {k: ts[k] for k in static_keys}

        pivot_data = zip(*[data[k] for k in pivot_keys])
        pivot_ts = zip(*[ts[k] for k in pivot_keys])


        for fr_no, (_data, _ts) in enumerate(zip(pivot_data, pivot_ts)):
            new_ev = {'uid': str(uuid.uuid4()),
                      'time': time,
                      'descriptor': inner_desc,
                      'seq_no': seq_no,
                      }
            inner_data = dict(static_data)
            inner_data.update({k: v for k, v in zip(pivot_keys, _data)})
            inner_data['fr_no'] = fr_no
            inner_ts = dict(static_ts)
            inner_ts['fr_no'] = ttime.time()
            new_ev['data'] = inner_data
            new_ev['timestamps'] = inner_ts
            yield new_ev
            seq_no += 1
