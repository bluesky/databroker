from __future__ import absolute_import
import six
import logging
logger = logging.getLogger(__name__)

try:
    import enaml

    from .model import MuxerModel

    with enaml.imports():
        from .view import MuxerController

except ImportError:
    if six.PY3:
        logger.exception('failed to import enaml')
    else:
        raise
