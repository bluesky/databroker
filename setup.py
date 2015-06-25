import sys
import warnings


try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup


setup(
    name='dataportal',
    version='0.1.0',
    author='Brookhaven National Laboratory',
    packages=['dataportal', 'dataportal.api', 'dataportal.testing',
              'dataportal.examples',
              'dataportal.examples.sample_data',
              'dataportal.broker', 'dataportal.muxer',
              'dataportal.sources', 'dataportal.sources.dummy_sources',
              'dataportal.utils', 'dataportal.scans',
              ],
)
