from . import conf, commands, odm_templates

__version__ = 'v0.1.0.post0'

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
