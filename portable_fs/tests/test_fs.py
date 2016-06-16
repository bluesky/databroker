import pytest
from portable_fs.template.core import DatumNotFound
from filestore.test.test_fs import (
    test_insert_funcs,
    test_root,
)
from filestore.test.test_fs_mutate import (
    test_root_shift,
    test_moving,
    test_over_step,
    test_history
)


def test_non_exist(fs):
      with pytest.raises(DatumNotFound):
          fs.retrieve('aardvark')
