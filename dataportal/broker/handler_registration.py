from filestore.api import register_handler
from filestore import handlers
from filestore.retrieve import HandlerBase
import logging


logger = logging.getLogger(__name__)


def register_builtin_handlers():
    "Register all the handlers built in to filestore."
    # TODO This will blow up if any non-leaves in the class heirarchy
    # have non-empty specs. Make this smart later.
    for cls in vars(handlers).values():
        if isinstance(cls, type) and issubclass(cls, HandlerBase):
            logger.debug("Found Handler %r for specs %r", cls, cls.specs)
            for spec in cls.specs:
                logger.debug("Registering Handler %r for spec %r", cls, spec)
                register_handler(spec, cls)
