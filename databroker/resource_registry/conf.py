import os
import yaml
import logging
from ..config import load_configuration
logger = logging.getLogger(__name__)

connection_config = None

connection_config = load_configuration('filestore', 'FS',
                                       ['host', 'database', 'port'])
