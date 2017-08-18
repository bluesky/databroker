import versioneer
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().split()

# Remove the 'optional' requirements
optional = ('h5py', 'pymongo', 'requests', 'tornado', 'ujson')
for package in optional:
    requirements.remove(package)

extras_require = {
    'mongo': ['pymongo'],
    'hdf5': ['h5py'],
    'client': ['requests'],
    'service': ['tornado', 'ujson'],
}

extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

setup(
    name='databroker',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    # Author details
    author='Brookhaven National Laboratory',

    packages=find_packages(),
    description='Unification of NSLS-II data sources',
    long_description=long_description,
    package_data={'databroker.assets': ['schemas/*.json']},
    # The project's main homepage.
    url='https://github.com/NSLS-II/databroker',
    scripts=['scripts/fs_rename', 'scripts/start_md_server'],
    license='BSD (3-clause)',

    install_requires=requirements,
    extras_require=extras_require,

    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
