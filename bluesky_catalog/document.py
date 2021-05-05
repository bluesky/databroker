import copy

from dask.base import normalize_token


class NotMutable(Exception):
    ...


class Document(dict):
    """
    Document is an immutable dict subclass.

    It is immutable to help consumer code avoid accidentally corrupting data
    that another part of the cosumer code was expected to use unchanged.

    Subclasses of Document must define __dask_tokenize__. The tokenization
    schemes typically uniquely identify the document based on only a subset of
    its contents, and mutating the contents can thereby create situations where
    two unequal objects have colliding tokens. Immutability helps guard against
    this too.

    Note that Documents are not *recursively* immutable. Just as it is possible
    create a tuple (immutable) of lists (mutable) and mutate the lists, it is
    possible to mutate the internal contents of a Document, but this should not
    be done. It is safer to use the to_dict() method to create a mutable deep
    copy.

    This is implemented as a dict subclass in order to satisfy certain
    consumers that expect an object that satisfies isinstance(obj, dict).
    This implementation detail may change in the future.
    """

    __slots__ = ("__not_a_real_dict",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # This lets pickle recognize that this is not a literal dict and that
        # it should respect its custom __setstate__.
        self.__not_a_real_dict = True

    def __repr__(self):
        # same as dict, but wrapped in the class name so the eval round-trips
        return f"{self.__class__.__name__}({dict(self)})"

    def _repr_pretty_(self, p, cycle):
        """
        A multi-line but eval-able text repr with readable indentation

        This hooks into IPython/Jupyter's display mechanism
        This is *not* invoked by print() or repr(), but it is invoked by
        IPython.display.display() which is called in this common scenario::

            In [1]: doc = Document(...)
            In [2]: doc
            <pretty representation will show here>
        """
        # Note: IPython's pretty-prettying mechanism is custom and complex.
        # The `text` method used below is a direct and blunt way to engage it
        # and seems widely used in the IPython code base. There are other
        # specific mechanisms for displaying collections like dicts, but they
        # can *truncate* which I think we want to avoid and they would require
        # more investment to understand how to use.
        from pprint import pformat

        return p.text(f"{self.__class__.__name__}({pformat(dict(self))})")

    def __getstate__(self):
        return dict(self)

    def __setstate__(self, state):
        dict.update(self, state)
        self.__not_a_real_dict = True

    def __readonly(self, *args, **kwargs):
        raise NotMutable(
            "Documents are not mutable. Call the method to_dict() to make a "
            "fully independent and mutable deep copy."
        )

    def __setitem__(self, key, value):
        try:
            self.__not_a_real_dict
        except AttributeError:
            # This path is necessary to support un-pickling.
            return dict.__setitem__(self, key, value)
        else:
            self.__readonly()

    __delitem__ = __readonly
    pop = __readonly
    popitem = __readonly
    clear = __readonly
    setdefault = __readonly
    update = __readonly

    def to_dict(self):
        """
        Create a mutable deep copy.
        """
        # Convert to dict and then make a deep copy to ensure that if the user
        # mutates any internally nested dicts there is no spooky action at a
        # distance.
        return copy.deepcopy(dict(self))

    def __deepcopy__(self, memo):
        # Without this, copy.deepcopy(Document(...)) fails because deepcopy
        # creates a new, empty Document instance and then tries to add items to
        # it.
        return self.__class__({k: copy.deepcopy(v, memo) for k, v in self.items()})

    def __dask_tokenize__(self):
        raise NotImplementedError


# We must use dask's registration mechanism to tell it to treat Document
# specially. Dask's tokenization dispatch mechanism discovers that Docuemnt is
# a dict subclass and treats it as a dict, ignoring its __dask_tokenize__
# method. To force it to respect our cutsom tokenization, we must explicitly
# register it.


@normalize_token.register(Document)
def tokenize_document(instance):
    return instance.__dask_tokenize__()


class Start(Document):
    def __dask_tokenize__(self):
        return ("start", self["uid"])


class Stop(Document):
    def __dask_tokenize__(self):
        return ("stop", self["uid"])


class Resource(Document):
    def __dask_tokenize__(self):
        return ("resource", self["uid"])


class Descriptor(Document):
    def __dask_tokenize__(self):
        return ("descriptor", self["uid"])


class Event(Document):
    def __dask_tokenize__(self):
        return ("event", self["uid"])


class EventPage(Document):
    def __dask_tokenize__(self):
        return ("event_page", self["uid"])


class Datum(Document):
    def __dask_tokenize__(self):
        return ("datum", self["datum_id"])


class DatumPage(Document):
    def __dask_tokenize__(self):
        return ("datum_page", self["uid"])
