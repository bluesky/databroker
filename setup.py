try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

import os


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='filestore',
    version='0.1.0.post0',
    license="BSD (3-clause)",
    url="https://github.com/NSLS-II/filestore",
    packages=['filestore', 'filestore.utils', 'filestore.readers'],
    package_data={'filestore': ['json/*.json']},
    long_description=read('README.md'),
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
    ],
    requires=['h5py', 'numpy', 'mongoengine', 'jsonschema']
)
