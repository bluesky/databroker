"""This module contains "PIMS readers" (see github.com/soft-matter/pims) that
take in headers and detector aliases and return a sliceable generator of arrays."""

from pims import FramesSequence, Frame
from . import DataBroker
from filestore.api import retrieve
from skimage import img_as_float


class Images(FramesSequence):
    def __init__(self, headers, name):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        headers : Header or list of Headers
        name : str
            alias (data key) of a detector

        Example
        -------
        >>> header = DataBroker[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        all_events = DataBroker.fetch_events(headers, fill=False)
        # TODO Make this smarter by only grabbing events from the relevant
        # descriptors. For now, we will do it this way to take advantage of
        # header validation logical happening in fetch_events.
        events = [event for event in all_events if name in event.data.keys()]
        self._datum_uids = [event.data[name] for event in events]
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
        return Frame(self.process_func(img), frame_no=i)


class SubtractedImages(FramesSequence):
    def __init__(self, headers, lightfield_name, darkfield_name):
        """
        Load images from a detector for given Header(s). Subtract
        dark images from each corresponding light image automatically.

        Parameters
        ----------
        headers : Header or list of Headers
        lightfield_name : str
            alias (data key) of lightfield images
        darkfield_name : str
            alias (data key) of darkfield images

        Example
        -------
        >>> header = DataBroker[-1]
        >>> images = SubtractedImages(header, 'my_lightfield', 'my_darkfield')
        >>> for image in images:
                # do something
        """
        self.light = Images(headers, lightfield_name)
        self.dark = Images(headers, darkfield_name)
        if len(self.light) != len(self.dark):
            raise ValueError("The streams from {0} and {1} have unequal "
                             "length and cannot be automatically subtracted.")
        self._len = len(self.light)
        example = img_as_float(self.light[0]) - img_as_float(self.dark[0])
        self._dtype = example.dtype
        self._shape = example.shape

    def get_frame(self, i):
        # Convert to float to avoid out-of-bounds wrap-around errors,
        # as in 10-11 = 255.
        return img_as_float(self.light[i]) - img_as_float(self.dark[i])

    @property
    def pixel_type(self):
        return self._dtype

    @property
    def frame_shape(self):
        return self._shape

    def __len__(self):
        return self._len
