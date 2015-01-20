import sys
import logging
from .sources import *


# Attach a default handler, printing INFO-level logs to stdout.
logger = logging.getLogger(__name__)
FORMAT = "%(name)s.%(funcName)s:  %(message)s"
formatter = logging.Formatter(FORMAT)
default_handler = logging.StreamHandler(sys.stdout)
default_handler.setLevel(logging.INFO)
default_handler.setFormatter(formatter)
logger.addHandler(default_handler)
logger.setLevel(logging.INFO)
