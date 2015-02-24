import numpy as numpy
import numpy as np  # support both 'numpy' and 'np' in evaluated string

def retrieve(eid):
    """
    Given a resource identifier return the data.

    Parameters
    ----------
    eid : str
        The resource ID (as stored in MDS)

    Returns
    -------
    data : ndarray
        The requested data as a numpy array
    """
    a = eval(eid)
    if not isinstance(a, np.ndarray):
        raise ValueError("The dummy version of retrieve data requires a string "
                         "of Python code that evalautes to an ndarray.")
    return a
