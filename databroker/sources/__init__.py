from importlib import import_module


__all__ = ['switch', 'channelarchiver']


def switch(channelarchiver=None):
    """
    Switch between using a real data source and a dummy version duplicating
    the API for the purposes of demostration, testing, or development.

    Parameters
    ----------
    channelarchiver : bool
        If False, use dummy channelarchiver.
    """
    format_string = 'databroker.sources.dummy_sources._{0}'
    for name, value in dict(channelarchiver=channelarchiver).items():
        if value is not None:
            if value:
                globals()[name] = import_module(name)
            else:
                globals()[name] = import_module(format_string.format(name))


# On importing databroker, set these defaults.
switch(channelarchiver=True)
