try:
    from setuptools import setup, find_packages
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

import versioneer
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='metadataservice',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license="BSD (3-clause)",
    url="https://github.com/nsls-ii/metadataservice",
    packages=find_packages(),
    long_description=read('README.md'),
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 3',
    ],
)
