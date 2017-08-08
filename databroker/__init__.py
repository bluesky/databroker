import warnings
import logging


logger = logging.getLogger(__name__)


try:
    import attr  # confusingly, this comes from 'attrs' on PyPI, not 'attr'
    attr.s
except AttributeError:
    raise ImportError("Wrong attr module imported. Please uninstall 'attr' "
                      "and install 'attrs' which provides the correct module.")


from .broker import Broker, ArchiverEventSource

# set version string using versioneer
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
