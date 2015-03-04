from __future__ import absolute_import
import six
import logging
logger = logging.getLogger(__name__)

try:
    import enaml

    from .model import GetLastModel, DisplayHeaderModel, WatchForHeadersModel

    with enaml.imports():
        from .view import (GetLastView, GetLastWindow, WatchForHeadersView,
                           DisplayHeaderView, DisplayPVView)

except ImportError:
    if six.PY3:
        logger.exception('Failed to import enaml, but this is expected '
                         'on python 3.  Eating exception and continuing.')
    else:
        raise
