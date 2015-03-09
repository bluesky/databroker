from .simple_broker import DataBroker, EventQueue
from .handler_registration import register_builtin_handlers

DataBroker = DataBroker()  # singleton
register_builtin_handlers()
