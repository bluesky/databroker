from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

import filestore.conf as fconf
import os
import yaml
import tempfile

from nose.tools import assert_equal


class test_config_read(object):
    demo_conf = {'database': 'aardvark',
                 'host': 'shadowbroker',
                 'port': 94}

    def setup(self):
        self._old_conf = fconf.connection_config
        with tempfile.NamedTemporaryFile(delete=False) as fout:
            self._fname = fout.name
            print(self.demo_conf)
            fout.write(yaml.dump(self.demo_conf, encoding='utf-8'))

    def teardown(self):
        os.unlink(self._fname)
        fconf.connection_config = self._old_conf

    def test_read_config(self):
        fconf.read_connection_config(self._fname)
        assert_equal(self.demo_conf, fconf.connection_config)
