from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

from .. import conf as fconf
import os
import yaml
import tempfile
import pytest


class Test_read_connection_config_file(object):
    @classmethod
    def setup(cls):
        # example contents of connection.yml
        cls.demo_conf = {'database': 'aardvark',
                         'host': 'shadowbroker',
                         'port': 94}
        # stash the existing configuration
        cls._old_conf = fconf.connection_config
        # make a yaml file wit the test data
        with tempfile.NamedTemporaryFile(delete=False) as fout:
            cls._fname = fout.name
            print(cls.demo_conf)
            fout.write(yaml.dump(cls.demo_conf, encoding='utf-8'))

    @classmethod
    def teardown(cls):
        # remove the test config file
        os.unlink(cls._fname)
        # restore the connection configuration...testing globals is fun!
        fconf.connection_config = cls._old_conf

    def test_read_config_file(self):
        fconf.connection_config = fconf.load_configuration('filestore', 'FS',
                                     ['host', 'database', 'port'],
                                     fname=self._fname)

        assert self.demo_conf == fconf.connection_config
