from __future__ import (absolute_import, division, print_function)


import sys
import warnings
try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

from distutils.core import setup, Extension
import numpy

MAJOR = 0
MINOR = 0
MICRO = 0
ISRELEASED = False
SNAPSHOT = False
VERSION = '%d.%d.%d' % (MAJOR, MINOR, MICRO)
QUALIFIER = ''

FULLVERSION = VERSION
print(FULLVERSION)

if not ISRELEASED:
    import subprocess
    FULLVERSION += '.dev'
    if SNAPSHOT:
        pipe = None
        for cmd in ['git', 'git.cmd']:
            try:
                pipe = subprocess.Popen([cmd, "describe", "--always",
                                         "--match", "v[0-9\/]*"],
                                        stdout=subprocess.PIPE)
                (so, serr) = pipe.communicate()
                print(so, serr)
                if pipe.returncode == 0:
                    pass
                print('here')
            except:
                pass
            if pipe is None or pipe.returncode != 0:
                warnings.warn("WARNING: Couldn't get git revision, "
                              "using generic version string")
            else:
                rev = so.strip()
                # makes distutils blow up on Python 2.7
                if sys.version_info[0] >= 3:
                    rev = rev.decode('ascii')

                # use result of git describe as version string
                FULLVERSION = VERSION + '-' + rev.lstrip('v')
                break
else:
    FULLVERSION += QUALIFIER

setup(
    name='replay',
    version=FULLVERSION,
    author='Brookhaven National Laboratory',
    packages=['replay',
              ],
)
