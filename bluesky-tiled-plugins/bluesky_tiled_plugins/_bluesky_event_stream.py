import keyword
import warnings

from tiled.client.container import DEFAULT_STRUCTURE_CLIENT_DISPATCH, Container

from ._common import IPYTHON_METHODS


class BlueskyEventStream(Container):
    """
    This encapsulates the data and metadata for one 'stream' in a Bluesky 'run'.

    This adds for bluesky-specific conveniences to the standard client Container.
    """

    def __repr__(self):
        return f"<{type(self).__name__} {set(self)!r} stream_name={self.metadata['stream_name']!r}>"

    @property
    def descriptors(self):
        return self.metadata["descriptors"]

    @property
    def _descriptors(self):
        # For backward-compatibility.
        # We do not normally worry about backward-compatibility of _ methods, but
        # for a time databroker.v2 *only* have _descriptors and not descriptros,
        # and I know there is useer code that relies on that.
        warnings.warn("Use .descriptors instead of ._descriptors.", stacklevel=2)
        return self.descriptors

    def __getattr__(self, key):
        """
        Let run.X be a synonym for run['X'] unless run.X already exists.

        This behavior is the same as with pandas.DataFrame.
        """
        # The wisdom of this kind of "magic" is arguable, but we
        # need to support it for backward-compatibility reasons.
        if key in IPYTHON_METHODS:
            raise AttributeError(key)
        if key in self:
            return self[key]
        raise AttributeError(key)

    def __dir__(self):
        # Build a list of entries that are valid attribute names
        # and add them to __dir__ so that they tab-complete.
        tab_completable_entries = [
            entry
            for entry in self
            if (entry.isidentifier() and (not keyword.iskeyword(entry)))
        ]
        return super().__dir__() + tab_completable_entries

    def read(self, *args, **kwargs):
        """
        Shortcut for reading the 'data' (as opposed to timestamps or config).

        That is:

        >>> stream.read(...)

        is equivalent to

        >>> stream["data"].read(...)
        """
        return self["data"].read(*args, **kwargs)

    def to_dask(self):
        warnings.warn(
            """Do not use this method.
Instead, set dask or when first creating the client, as in

    >>> catalog = from_uri("...", "dask")

and then read() will return dask objects.""",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.new_variation(
            structure_clients=DEFAULT_STRUCTURE_CLIENT_DISPATCH["dask"]
        ).read()
