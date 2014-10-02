#!/usr/bin/env python

import setuptools
from distutils.core import setup, Extension
import numpy as np

setup(
    name='DataBroker',
    version='0.0.x',
    author='Brookhaven National Lab',
    packages=["databroker",
              ],
    include_dirs=[np.get_include()],
    )
