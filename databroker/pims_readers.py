"""This module contains "PIMS readers" (see github.com/soft-matter/pims) that
take in headers and detector aliases and return a sliceable generator of arrays."""

from collections import defaultdict
from pims import FramesSequence, Frame
from . import get_events
from filestore.api import retrieve

def get_images(headers, name, handler_registry=None, handler_override=None):
    """
    Load images from a detector for given Header(s).

    Parameters
    ----------
    headers : Header or list of Headers
    name : string
        field name (data key) of a detector
    handler_registry : dict, optional
        mapping spec names (strings) to handlers (callable classes)
    handler_override : callable class, optional
        overrides registered handlers
        

    Example
    -------
    >>> header = DataBroker[-1]
    >>> images = Images(header, 'my_detector_lightfield')
    >>> for image in images:
            # do something
    """
    return Images(headers, name, handler_registry, handler_override)


class Images(FramesSequence):
    def __init__(self, headers, name, handler_registry=None,
                 handler_override=None):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        headers : Header or list of Headers
        name : str
            field name (data key) of a detector
        handler_registry : dict, optional
            mapping spec names (strings) to handlers (callable classes)
        handler_override : callable class, optional
            overrides registered handlers

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
        first_uid = self._datum_uids[0]
        if handler_override is None:
            self.handler_registry = handler_registry
        else:
            # mock a handler registry
            self.handler_registry = defaultdict(lambda: handler_override)
        example_frame = retrieve(first_uid, self.handler_registry)
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
        img = retrieve(self._datum_uids[i], self.handler_registry)
        return Frame(img, frame_no=i)
