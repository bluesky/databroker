try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='metadatastore',
    version='0.1.0',
    license="BSD (3-clause)",
    url="https://github.com/NSLS-II/metadatastore",
    packages=['metadatastore', 'metadatastore.test', 'metadatastore.utils',
              'metadatastore.examples', 'metadatastore.examples.sample_data'],
    long_description=read('README.md'),
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 3',
    ],
)
