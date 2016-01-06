from __future__ import absolute_import, division, print_function

from .api import (db_connect, db_disconnect, insert_resource, insert_datum,
                  bulk_insert_datum, retrieve)
import warnings

warnings.warn("Do not import filestore.commands, "
              "import filestore.api instead")
