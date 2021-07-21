from pims import FramesSequence, Frame


class Images(FramesSequence):
    def __init__(self, data_array):
        """
        This class is deprecated.

        Load images from a detector for given Header(s).

        Parameters
        ----------
        data_array : xarray.DataArray

        Example
        -------
        >>> header = db[-1]
        >>> images = Images(header, 'my_detector_lightfield')
        >>> for image in images:
                # do something
        """
        # warn("Images and get_images are deprecated. Use Header.data(), "
        #      "Header.xarray() or Header.xarray_dask() instead.", stacklevel=3)
        self._data_array = data_array
        self._dtype = data_array.dtype
        self._shape = data_array.shape[1:]
        self._len = len(data_array)

    @property
    def pixel_type(self):
        return self._dtype

    @property
    def frame_shape(self):
        return self._shape

    def __len__(self):
        return self._len

    def get_frame(self, i):
        img = self._data_array[i]
        return Frame(img.data, frame_no=i)
