from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

# needed for API
from .core import DatumNotFound
from .handlers_base import HandlerBase, DuplicateHandler

from .api import (handler_context, register_handler,
                  deregister_handler, get_spec_handler, get_data)
import warnings


warnings.warn("Do not import filestore.retrieve, "
              "import filestore.api instead")
