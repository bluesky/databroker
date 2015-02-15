from __future__ import absolute_import
import six
import logging
logger = logging.getLogger(__name__)

try:
    import enaml


    with enaml.imports():
        from .view import PlotView, PlotControls

    from .model import ScalarCollection

except ImportError:
    if six.PY3:
        logger.exception('Failed to import enaml, but this is expected '
                         'on python 3.  Eating exception and continuing.')
    else:
        raise
