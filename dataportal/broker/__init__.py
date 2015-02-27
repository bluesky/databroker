from .simple_broker import DataBroker
from .handler_registration import register_builtin_handlers

DataBroker = DataBroker()  # singleton
register_builtin_handlers()
