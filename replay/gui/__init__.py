
import logging
logger = logging.getLogger(__name__)

from logging import NullHandler
logger.addHandler(NullHandler())