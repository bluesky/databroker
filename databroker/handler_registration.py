from .databroker import DataBroker
from .core import register_builtin_handlers as _register_builtin_handlers


def register_builtin_handlers():
    return _register_builtin_handlers(DataBroker.fs)
