import filestore
import warnings


def test_top_level_imports():
    filestore.FileStore
    filestore.FileStoreRO
    filestore.DatumNotFound


def test_import_warnings():
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("always")
        # Trigger a warning.
        import filestore.commands
        import filestore.retrieve
        assert len(w) == 2
