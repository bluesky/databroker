from __future__ import absolute_import
import six
import logging
logger = logging.getLogger(__name__)
from .. import py3_errmsg

try:
    import enaml
except ImportError:
    if six.PY3:
        logger.exception(py3_errmsg)
    else:
        raise
else:
    from .model import MuxerModel
    with enaml.imports():
        from .view import MuxerController
