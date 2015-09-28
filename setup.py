import sys
import warnings
import versioneer


try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup


setup(
    name='dataportal',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author='Brookhaven National Laboratory',
    packages=['dataportal', 'dataportal.testing',
              'dataportal.examples',
              'dataportal.examples.sample_data',
              'dataportal.broker',
              'dataportal.sources', 'dataportal.sources.dummy_sources',
              'dataportal.utils',
              ],
)
