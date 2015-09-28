from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from collections import OrderedDict
import importlib
import sys
import six


def watermark():
    """
    Give the version of each of the dependencies -- useful for bug reports.

    Returns
    -------
    result : dict
        mapping the name of each package to its version string or, if an
        optional dependency is not installed, None
    """
    packages = ['six', 'numpy', 'scipy', 'matplotlib', 'pandas', 'pims',
                'pyyaml', 'metadatastore', 'filestore',
                'channelarchiver', 'xray_vision']
    result = OrderedDict()
    for package_name in packages:
        try:
            package = importlib.import_module(package_name)
            version = package.__version__
        except ImportError:
            result[package_name] = None
        except Exception as err:
            version = "FAILED TO DETECT: {0}".format(err)
        result[package_name] = version

    # ...as does Python
    version_info = sys.version_info
    result['python'] = _make_version_string(version_info)
    return result

def _make_version_string(version_info):
    version_string = '.'.join(map(str, [version_info[0], version_info[1],
                                  version_info[2]]))
    return version_string
