import numpy as np
import uuid
from functools import wraps
from metadatastore.api import (insert_run_start, insert_beamline_config,
                               insert_run_stop, Document)


def stepped_ramp(start, stop, step, points_per_step, noise_level=0.1):
    """
    Simulate a stepped ramp.
    """
    rs = np.random.RandomState(0)
    data = np.repeat(np.arange(start, stop, step), points_per_step)
    noise = step * noise_level * rs.randn(len(data))
    noisy_data = data + noise
    return noisy_data


def apply_deadband(data, band):
    """
    Turn a stream of regularly spaced data into an intermittent stream.

    This simulates a deadband, where each data point is only included if
    it is significantly different from the previously included data point.

    Parameters
    ----------
    data : ndarray
    band : float
        tolerance, the width of the deadband

    Returns
    -------
    result : tuple
        indicies, data
    """
    if band < 0:
        raise ValueError("The width of the band must be nonnegative.")
    # Eric and Dan can't think of a way to vectorize this.
    set_point = data[0]
    # Always include the first point.
    indicies = [0]
    significant_data = [data[0]]
    for i, point in enumerate(data[1:]):
        if abs(point - set_point) > band:
            indicies.append(1 + i)
            significant_data.append(point)
            set_point = point
    return indicies, significant_data


def noisy(val, sigma=0.01):
    """Return a copy of the input plus noise

    Parameters
    ----------
    val : number or ndarrray
    sigma : width of Gaussian from which noise values are drawn

    Returns
    -------
    noisy_val : number or ndarray
        same shape as input val
    """
    if np.isscalar(val):
        return val + sigma * np.random.randn()
    else:
        return val + sigma * np.random.randn(val.shape)


def example(func):
    @wraps(func)
    def mock_run_start(run_start=None, sleep=0):
        if run_start is None:
            blc = insert_beamline_config({}, time=0.)
            run_start = insert_run_start(time=0., scan_id=1, beamline_id='csx',
                                         uid=str(uuid.uuid4()),
                                         beamline_config=blc)
        events = func(run_start, sleep)
        # Infer the end run time from events, since all the times are
        # simulated and not necessarily based on the current time.
        time = max([event.time for event in events])
        insert_run_stop(run_start, time=time, exit_status='success')
        events = [Document(e) for e in events]
        return events
    return mock_run_start
