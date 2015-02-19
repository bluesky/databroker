from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from collections import OrderedDict
import importlib
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
                'channelarchiver', 'bubblegum']
    result = OrderedDict()
    for package_name in packages:
        try:
            package = importlib.import_module(package_name)
        except ImportError:
            result[package_name] = None
        else:
            version = package.__version__

    # enaml provides its version differently
    try:
        import enaml
    except ImportError:
        result['enaml'] = None
    else:
        from enaml.version import version_info
        result['enaml'] = _make_version_string(version_info)

    # ...as does Python
    version_info = sys.version_info
    result['python'] = _make_version_string(version_info)
    return result

def _make_version_string(version_info):
    version_string = '.'.join(map(str, [version_info[0], version_info[1],
                                  version_info[2]]))
    return version_string
