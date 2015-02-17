#!/usr/bin/env python
from __future__ import (absolute_import, division, print_function)

import sys
import warnings
import numpy as np


try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

from distutils.core import setup, Extension

MAJOR = 0
MINOR = 0
MICRO = 1
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
    name='dataportal',
    version=FULLVERSION,
    author='Brookhaven National Laboratory',
    packages=['dataportal', 'dataportal.api', 'dataportal.testing',
              'dataportal.examples',
              'dataportal.examples.sample_data',
              'dataportal.broker', 'dataportal.muxer',
              'dataportal.sources', 'dataportal.sources.dummy_sources',
              'dataportal.replay', 'dataportal.replay.muxer',
              'dataportal.replay.scalar', 'dataportal.replay.search'
              ],
    include_dirs=[np.get_include()],
    entry_points={
        'console_scripts': [
            'replay = dataportal.replay.replay:main']},
    package_data={'': ['*.enaml']},
    include_package_data=True,
    requires=['skxray', 'pandas', 'matplotlib', 'enaml', 'bubblegum',
              'pims', 'six'],
)
