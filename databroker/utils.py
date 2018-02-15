from datetime import datetime
import numpy as np
import os
import pytz
import six
import warnings


class ALL:
    "Sentinel used as the default value for stream_name"
    pass


if six.PY2:
    # http://stackoverflow.com/a/5032238/380231
    def ensure_path_exists(path, exist_ok=True):
        import errno
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST or not exist_ok:
                raise
else:
    # technically, this won't work with py3.1, but no one uses that
    def ensure_path_exists(path, exist_ok=True):
        return os.makedirs(path, exist_ok=exist_ok)


def sanitize_np(val):
    "Convert any numpy objects into built-in Python types."
    if isinstance(val, (np.generic, np.ndarray)):
        if np.isscalar(val):
            return val.item()
        return val.tolist()
    return val


def apply_to_dict_recursively(d, f):
    for key, val in d.items():
        if hasattr(val, 'items'):
            d[key] = apply_to_dict_recursively(val, f)
        d[key] = f(val)


def format_time(search_dict, tz):
    """Helper function to format the time arguments in a search dict

    Expects 'since' and 'until'

    ..warning: Does in-place mutation of the search_dict
    """
    # The old names of 'since' and 'until' are 'start_time' and 'stop_time'.
    if 'since' in search_dict and 'start_time' in search_dict:
        raise TypeError("cannot use both 'since' and its deprecated name "
                        "'start_time'")
    if 'until' in search_dict and 'stop_time' in search_dict:
        raise TypeError("cannot use both 'until' and its deprecated name "
                        "'stop_time'")
    if 'start_time' in search_dict or 'stop_time' in search_dict:
        warnings.warn("The keyword 'start_time' and 'stop_time' have been "
                      "renamed to 'since' and 'until'. The old names are "
                      "deprecated.")
    time_dict = {}
    since = search_dict.pop('since', search_dict.pop('start_time', None))
    until = search_dict.pop('until', search_dict.pop('stop_time', None))
    if since:
        time_dict['$gte'] = normalize_human_friendly_time(since, tz)
    if until:
        time_dict['$lte'] = normalize_human_friendly_time(until, tz)
    if time_dict:
        search_dict['time'] = time_dict


# human friendly timestamp formats we'll parse
_TS_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',  # these 2 are not as originally doc'd,
    '%Y-%m-%d %H',     # but match previous pandas behavior
    '%Y-%m-%d',
    '%Y-%m',
    '%Y']

# build a tab indented, '-' bulleted list of supported formats
# to append to the parsing function docstring below
_doc_ts_formats = '\n'.join('\t- {}'.format(_) for _ in _TS_FORMATS)


def normalize_human_friendly_time(val, tz):
    """Given one of :
    - string (in one of the formats below)
    - datetime (eg. datetime.now()), with or without tzinfo)
    - timestamp (eg. time.time())
    return a timestamp (seconds since jan 1 1970 UTC).

    Non string/datetime values are returned unaltered.
    Leading/trailing whitespace is stripped.
    Supported formats:
    {}
    """
    # {} is placeholder for formats; filled in after def...

    zone = pytz.timezone(tz)  # tz as datetime.tzinfo object
    epoch = pytz.UTC.localize(datetime(1970, 1, 1))
    check = True

    if isinstance(val, six.string_types):
        # unix 'date' cmd format '%a %b %d %H:%M:%S %Z %Y' works but
        # doesn't get TZ?

        # Could cleanup input a bit? remove leading/trailing [ :,-]?
        # Yes, leading/trailing whitespace to match pandas behavior...
        # Actually, pandas doesn't ignore trailing space, it assumes
        # the *current* month/day if they're missing and there's
        # trailing space, or the month is a single, non zero-padded digit.?!
        val = val.strip()

        for fmt in _TS_FORMATS:
            try:
                ts = datetime.strptime(val, fmt)
                break
            except ValueError:
                pass

        try:
            if isinstance(ts, datetime):
                val = ts
                check = False
            else:
                # what else could the type be here?
                raise TypeError('expected datetime,'
                                ' got {:r}'.format(ts))

        except NameError:
            raise ValueError('failed to parse time: ' + repr(val))

    if check and not isinstance(val, datetime):
        return val

    if val.tzinfo is None:
        # is_dst=None raises NonExistent and Ambiguous TimeErrors
        # when appropriate, same as pandas
        val = zone.localize(val, is_dst=None)

    return (val - epoch).total_seconds()


# fill in the placeholder we left in the previous docstring
normalize_human_friendly_time.__doc__ = (
    normalize_human_friendly_time.__doc__.format(_doc_ts_formats)
)
