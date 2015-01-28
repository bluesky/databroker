__author__ = 'arkilic'

# import to do registering so things 'just work'
from .. import retrieve as fsr
from ..file_readers import NpyHandler

# register npy handler
fsr.register_handler('npy', NpyHandler)

# clean up to not pollute namespace
del fsr
del NpyHandler
