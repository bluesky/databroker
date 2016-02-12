"""This module contains "PIMS readers" (see github.com/soft-matter/pims) that
take in headers and detector aliases and return a sliceable generator of arrays."""

from pims import FramesSequence, Frame
from . import get_events
from filestore.api import retrieve

def get_images(headers, name):
    """
    Load images from a detector for given Header(s).

    Parameters
    ----------
    headers : Header or list of Headers
    name : string
        field name (data key) of a detector

    Example
    -------
    >>> header = DataBroker[-1]
    >>> images = Images(header, 'my_detector_lightfield')
    >>> for image in images:
            # do something
    """
    return Images(headers, name)


class Images(FramesSequence):
    def __init__(self, headers, name):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        headers : Header or list of Headers
        name : str
            field name (data key) of a detector

        Example
        -------
        >>> header = DataBroker[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        events = get_events(headers, [name], fill=False)
        self._datum_uids = [event.data[name] for event in events
                            if name in event.data]
        self._len = len(self._datum_uids)
        example_frame = retrieve(self._datum_uids[0])
        self._dtype = example_frame.dtype
        self._shape = example_frame.shape

    @property
    def pixel_type(self):
        return self._dtype

    @property
    def frame_shape(self):
        return self._shape

    def __len__(self):
        return self._len

    def get_frame(self, i):
        img = retrieve(self._datum_uids[i])
        return Frame(img, frame_no=i)
