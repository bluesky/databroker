"""This module contains "PIMS readers" (see github.com/soft-matter/pims) that
take in headers and detector aliases and return a sliceable generator of arrays."""

from pims import FramesSequence, Frame
from . import DataBroker
from filestore.api import retrieve
from skimage import img_as_float


class Images(FramesSequence):
    def __init__(self, headers, name, process_func=None, dtype=None,
                 as_grey=False):
        """
        Load images from a detector for given Header(s).

        Parameters
        ----------
        headers : Header or list of Headers
        name : str
            alias (data key) of a detector
        process_func: callable, optional
            function to be applied to each image
        dtype : numpy.dtype or str, optional
            data type to cast each image as
        as_grey : boolean, optional
            False by default
            quick-and-dirty way to ensure images are reduced to greyscale
            To take more control over how conversion is performed,
            use process_func above.

        Example
        -------
        >>> header = DataBroker[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        self._dtype = dtype
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

        self._validate_process_func(process_func)
        self._as_grey(as_grey, process_func)

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
        if self._dtype is not None and img.dtype != self._dtype:
            img = img.astype(self._dtype)
        return Frame(self.process_func(img), frame_no=i)


class SubtractedImages(FramesSequence):
    def __init__(self, headers, lightfield_name, darkfield_name,
                 process_func=None, dtype=None, as_grey=False):
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
        process_func: callable, optional
            function to be applied to each image
        dtype : numpy.dtype or str, optional
            data type to cast each image as
        as_grey : boolean, optional
            False by default
            quick-and-dirty way to ensure images are reduced to greyscale
            To take more control over how conversion is performed,
            use process_func above.

        Example
        -------
        >>> header = DataBroker[-1]
        >>> images = SubtractedImages(header, 'my_lightfield', 'my_darkfield')
        >>> for image in images:
                # do something
        """
        self.light = Images(
                headers, lightfield_name, process_func, dtype, as_grey)
        self.dark = Images(
                headers, darkfield_name, process_func, dtype, as_grey)
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
