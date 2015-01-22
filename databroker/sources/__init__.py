from collections import OrderedDict
from importlib import import_module
import logging


logger = logging.getLogger(__name__)
source_names = ['channelarchiver', 'metadataStore', 'metadataStore.api',
                'metadataStore.api.analysis', 'fileStore', 'fileStore.commands']
__all__ = ['switch'] + source_names


def switch(channelarchiver=None, metadatastore=None, filestore=None):
    """
    Switch between using a real data source and a dummy version duplicating
    the API for the purposes of demostration, testing, or development.

    Parameters
    ----------
    channelarchiver : bool
        If False, use dummy channelarchiver.
    """
    format_string = 'databroker.sources.dummy_sources._{0}'
    sources = OrderedDict()
    for name in source_names:
        kwarg = name.lower().split('.')[0]
        sources[name] = vars()[kwarg]
    for name, value in dict(**sources).items():
        if value is not None:
            if value:
                globals()[name] = import_module(name)
            else:
                globals()[name] = import_module(format_string.format(name))
                logger.debug('Pointing %s to %s', name, globals()[name])


# On importing databroker, set these defaults.
switch(channelarchiver=False, metadatastore=False, filestore=True)
