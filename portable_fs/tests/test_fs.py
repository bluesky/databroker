import pytest
from portable_fs.template.core import DatumNotFound
from filestore.test.test_fs import (
    test_insert_funcs,
    test_root,
)


def test_non_exist(fs):
      with pytest.raises(DatumNotFound):
          fs.retrieve('aardvark')
