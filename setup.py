import versioneer
from setuptools import setup, find_packages
import sys

# To use a consistent encoding
from codecs import open
from os import path

# NOTE: This file must remain Python 2 compatible for the foreseeable future,
# to ensure that we error out properly for people with outdated setuptools
# and/or pip.
min_version = (3, 6)
if sys.version_info < min_version:
    error = """
databroker does not support Python {0}.{1}.
Python {2}.{3} and above is required. Check your Python version like so:

python3 --version

This may be due to an out-of-date pip. Make sure you have pip >= 9.0.1.
Upgrade pip like so:

pip install --upgrade pip
""".format(*(sys.version_info[:2] + min_version))
    sys.exit(error)

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


def read_requirements(filename):
    with open(path.join(here, filename)) as requirements_file:
        # Parse requirements-*.txt, ignoring any commented-out lines.
        requirements = [
            line
            for line in requirements_file.read().splitlines()
            if not line.startswith("#")
        ]
    return requirements


suffixes = ["client", "server", "back-compat", "docs", "test"]
extras_require = {
    suffix: read_requirements(f"requirements-{suffix}.txt") for suffix in suffixes
}
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))
extras_require["all"] = extras_require["complete"]  # for back-compat

setup(
    name='databroker',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    # Author details
    author='Brookhaven National Laboratory',

    packages=find_packages(),
    description='Unification of NSLS-II data sources',
    long_description=long_description,
    long_description_content_type='text/markdown',
    package_data={'databroker.assets': ['schemas/*.json']},
    # The project's main homepage.
    url='https://github.com/NSLS-II/databroker',
    scripts=['scripts/fs_rename'],
    license='BSD (3-clause)',
    install_requires=['tiled[minimal-client]'],
    extras_require=extras_require,
    python_requires='>={}'.format('.'.join(str(n) for n in min_version)),
    entry_points={
        "console_scripts": ["databroker = databroker.cli:main"],
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
