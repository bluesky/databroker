from pims import FramesSequence
from ..broker import DataBroker
from filestore.api import retrieve

class Images(FramesSequence):
    def __init__(self, headers, name, process_func=None, dtype=None, as_grey=False):
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
        events = DataBroker.fetch_events(headers, fill=False)
        self._datum_uids = [event.data[name] for event in events]

        self._validate_process_func(process_func)
        self._as_grey(as_grey, process_func)

    def get_frame(self, i):
        img = retrieve(self._datum_uids[i])
        if self._dtype is not None and img.dtype != self._dtype:
            img = img.astype(self._dtype)
        return Frame(self.process_func(img), frame_no=i)
