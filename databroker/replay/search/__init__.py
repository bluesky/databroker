__author__ = 'edill'
import enaml

from .model import LastModel

with enaml.imports():
    from .view import LastView
