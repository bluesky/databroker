import six
import os


if six.PY2:
    # http://stackoverflow.com/a/5032238/380231
    def ensure_path_exists(path):
        import errno
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
else:
    # technically, this won't work with py3.1, but no one uses that
    def ensure_path_exists(path):
        return os.makedirs(path, exist_ok=True)
