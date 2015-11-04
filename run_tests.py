#!/usr/bin/env python

import nose

plugins = []
env = {"NOSE_WITH_COVERAGE": 1,
       'NOSE_COVER_PACKAGE': 'metadataclient',
       'NOSE_COVER_HTML': 1}
# Nose doesn't automatically instantiate all of the plugins in the
# child processes, so we have to provide the multiprocess plugin with
# a list.
from nose.plugins import multiprocess
multiprocess._instantiate_plugins = plugins


def run():

    nose.main(addplugins=[x() for x in plugins], env=env)


if __name__ == '__main__':
    run()
