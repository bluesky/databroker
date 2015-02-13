from __future__ import unicode_literals, print_function, absolute_import
import six
import numpy as np
import enaml
from datetime import datetime
__author__ = 'edill'

def initialize_view(view):
    try:
        view.show()
    except RuntimeError as re:
        import sys
        err_msg = ("\n"
                   "\nOriginal message"
                   "\n----------------"
                   "\n{}"
                   "\n"
                   "\nHelpfulMessage"
                   "\n--------------"
                   "\nHave you created an active Qt application?  Try:"
                   "\n"
                   "\nfrom enaml.qt.qt_application import QtApplication"
                   "\napp = QtApplication()"
                   "\nmodel = make_cross_section_view()"
                   "\napp.start()"
                   "".format(re))
        six.reraise(RuntimeError, RuntimeError(err_msg), sys.exc_info()[2])

def make_line_view(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : replay.core.image_model
    window : replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    from ..model.scalar_model import ScalarCollection

    with enaml.imports():
        from .line_view import LineView

    x = np.arange(0, 10, .01)
    y = np.sin(x)
    xy = {'init': (x.tolist(), y.tolist())}
    line_model = ScalarCollection(xy=xy)
    line_view = LineView(line_model=line_model)
    # if init_view:
    #     initialize_view(line_view)
    return line_model, line_view


def make_line_window(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : dataportal.replay.core.image_model
    window : dataportal.replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    from ..model.scalar_model import ScalarCollection

    with enaml.imports():
        from ..gui.line_view import LineWindow

    line_model = ScalarCollection()
    line_view = LineWindow(line_model=line_model)
    # if init_view:
    #     initialize_view(line_view)
    return line_model, line_view

def make_image_view(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : dataportal.replay.core.image_model
    window : dataportal.replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    from ..model.image_model import ImageModel

    with enaml.imports():
        from ..gui.image_view import ImView

    image_model = ImageModel()
    image_view = ImView(image_model=image_model)
    if init_view:
        initialize_view(image_view)
    return image_model, image_view

def make_image_window(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : dataportal.replay.core.image_model
    window : dataportal.replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    from ..model.image_model import ImageModel

    with enaml.imports():
        from ..gui.image_view import ImWindow

    image_model = ImageModel()
    image_view = ImWindow(image_model=image_model)
    if init_view:
        initialize_view(image_view)
    return image_model, image_view


def make_cross_section_view(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : dataportal.replay.core.image_model
    window : dataportal.replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    from ..model.cross_section_model import CrossSectionModel
    from ...muggler.data import DataMuggler

    with enaml.imports():
        from ..gui.cross_section_view import CrossSectionMain

    dm = DataMuggler((('T', 'pad', True),
                      ('img', None, False),
                      ('count', None, True)
                      )
    )
    pixels = 1000
    data = [np.random.rand(pixels,pixels) * 10 * np.random.rand()
            for _ in range(10)]
    for img in data:
        dm.append_data(datetime.utcnow(), data_dict = {'img': img})
    xs_model = CrossSectionModel(data_muggler=dm, name='img')
    xs_view = CrossSectionMain(cs_model=xs_model)
    if init_view:
        initialize_view(xs_view)
        xs_model.image_index = len(xs_model.sliceable_data)-1
    return xs_model, xs_view

def make_param_view(data_muggler, init_view=True):
    """

    Create a VariableView and its VariableModel. Call VariableView.show()
    and return the VariableView and its VariableModel.

    Returns
    -------
    model : dataportal.replay.core.image_model
    window : dataportal.replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    from ..model.scalar_model import ScalarCollection

    with enaml.imports():
        from ..gui.variable_view import VariableMain
    pixels = 1000
    data = [np.random.rand(pixels,pixels) for _ in range(10)]
    var_model = ScalarCollection(data_muggler=data_muggler)
    var_view = VariableMain(model=var_model)
    if init_view:
        initialize_view(var_view)
        var_model.image_index = len(var_model.data)-1
    return var_model, var_view
