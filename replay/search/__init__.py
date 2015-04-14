from __future__ import absolute_import
import six
import logging
from .. import py3_errmsg


logger = logging.getLogger(__name__)

try:
    import enaml
except ImportError:
    if six.PY3:
        logger.exception(py3_errmsg)
    else:
        raise
else:
    from .model import (GetLastModel, DisplayHeaderModel, WatchForHeadersModel,
                        ScanIDSearchModel)

    with enaml.imports():
        from .view import (GetLastView, GetLastWindow, WatchForHeadersView,
                           DisplayHeaderView, DisplayPVView, ScanIDSearchView)
