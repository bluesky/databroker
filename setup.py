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
    name='databroker',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author='Brookhaven National Laboratory',
    packages=['databroker', 'databroker.testing',
              'databroker.examples',
              'databroker.examples.sample_data',
              'databroker.broker',
              'databroker.sources', 'databroker.sources.dummy_sources',
              'databroker.utils',
              ],
)
