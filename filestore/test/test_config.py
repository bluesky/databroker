from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

import filestore.conf as fconf
import os
import yaml
import tempfile

from nose.tools import assert_equal


class test_read_connection_config_file(object):
    # example contents of connection.yml
    demo_conf = {'database': 'aardvark',
                 'host': 'shadowbroker',
                 'port': 94}

    def setup(self):
        # stash the existing configuration
        self._old_conf = fconf.connection_config
        # make a yaml file wit the test data
        with tempfile.NamedTemporaryFile(delete=False) as fout:
            self._fname = fout.name
            print(self.demo_conf)
            fout.write(yaml.dump(self.demo_conf, encoding='utf-8'))

    def teardown(self):
        # remove the test config file
        os.unlink(self._fname)
        # restore the connection configuration...testing globals is fun!
        fconf.connection_config = self._old_conf

    def test_read_config_file(self):
        fconf.connection_config = fconf.load_configuration('filestore', 'FS',
                                     ['host', 'database', 'port'],
                                     fname=self._fname)

        assert_equal(self.demo_conf, fconf.connection_config)
