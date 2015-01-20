import six
from common import beamline_aliases

def beamline_id(value):
    if isinstance(value, six.string_types):
        value = value.lower()
        if value in beamline_aliases.keys():
            return beamline_aliases[value]
        elif value in beamline_aliases.values():
            return value
    raise ValueError("The configuration argument must be a beamline "
                     "code like '23id' or common alias like 'csx'. "
                     "Capitalization does not matter.")
