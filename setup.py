#!/usr/bin/env python

import setuptools
from distutils.core import setup, Extension
import numpy as np

setup(
    name='DataBroker',
    version='0.0.x',
    author='Brookhaven National Lab',
    packages=["databroker", 'databroker.api',
              'databroker.server', 'databroker.commands'
              ],
    include_dirs=[np.get_include()],
    )
