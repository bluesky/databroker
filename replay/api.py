from __future__ import unicode_literals, print_function, absolute_import
import six
import numpy as np
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
    import numpy as np
    import enaml
    from enaml.qt.qt_application import QtApplication
    from replay.core import LineModel, ImageModel

    with enaml.imports():
        from replay.gui.core import LineView

    x = np.arange(0, 10, .01)
    y = np.sin(x)
    line_model = LineModel(x=x, y=y)
    line_view = LineView(line_model=line_model)
    if init_view:
        initialize_view(line_view)
    return line_model, line_view

def make_image_view(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : replay.core.image_model
    window : replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    import enaml
    from replay.core import ImageModel

    with enaml.imports():
        from replay.gui.core import ImView

    image_model = ImageModel()
    image_view = ImView(image_model=image_model)
    if init_view:
        initialize_view(image_view)
    return image_model, image_view


def make_cross_section_view(init_view=True):
    """

    Create a LineView and its LineModel. Call LineView.show() and return
    LineModel which is the back-end for the LineView

    Returns
    -------
    model : replay.core.image_model
    window : replay.gui.core.ImageView
        enaml.widgets.api.MainWindow
    """
    import enaml
    from replay.cross_section_model import CrossSectionModel

    with enaml.imports():
        from replay.gui.cross_section_view import CrossSectionMain
    pixels = 1000
    data = [np.random.rand(pixels,pixels) for _ in range(10)]
    xs_model = CrossSectionModel(data=data)
    xs_view = CrossSectionMain(model=xs_model)
    if init_view:
        initialize_view(xs_view)
    return xs_model, xs_view
