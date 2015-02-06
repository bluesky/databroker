import enaml

from .model import MugglerModel

with enaml.imports():
    from .view import MugglerController
