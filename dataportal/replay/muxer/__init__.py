import enaml

from .model import MuxerModel

with enaml.imports():
    from .view import MuxerController
