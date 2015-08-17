from __future__ import absolute_import

from . import conf

__version__ = '0.1.0.post0'

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
