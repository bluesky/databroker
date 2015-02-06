import enaml

from .model import ScalarCollection

with enaml.imports():
    from .view import ScalarContainer, ScalarView, ScalarController
