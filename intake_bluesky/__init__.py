# Import intake to run driver discovery first and avoid circular import issues.
import intake
from ._version import get_versions
__version__ = get_versions()['version']
del intake
del get_versions
