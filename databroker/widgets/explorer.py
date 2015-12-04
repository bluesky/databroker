from databroker import DataBroker as db, get_events, get_table
from ipywidgets import (interact, Text, Dropdown, Checkbox, VBox, HBox,
                        SelectMultiple, ToggleButtons, Select)
import matplotlib.pyplot as plt
from collections import namedtuple
from traitlets import Tuple, Unicode, HasTraits, Set, Int, link

import logging
from collections import namedtuple
logger = logging.getLogger(__name__)



def explorer(ax):
    """A databroker explorer widget. See below for suggested usage

    Parameters
    ----------
    ax : matplotlib.Axes
        Axes that these data should be plotted into.
        Note that this widget will call ax.cla() frequently on this axis.

    Returns
    -------
    widgets : dict
        Dictionary of all the ipython widgets that were created for this
        explorer widget
    display : ipywidgets.widgets.widget_box.FlexBox
        The thing that contains the control widgets

    Notes
    -----
    Copy/paste this code block into a notebook and execute the cell ::

        from databroker.widgets import explorer
        %matplotlib notebook
        widgets, display = explorer.explorer()
        display
    """

    def _has_hyphen(scan):
        start, stop = scan.split('-')
        return list(range(int(start), int(stop)+1))

    def _has_colon(scan):
        try:
            start, stop, step = scan.split(':')
        except ValueError:
            start, stop = scan.split(':')
            step = 1
        return list(range(int(start), int(stop)+1, int(step)))


    def scan_submit(sender):
        all_scans = get_scans(scan_text.value)
        logger.debug('all_scans = %s', all_scans)
        valid_scans = set()
        for scan in all_scans:
            if scan in data:
                valid_scans.add(scan)
                # skip scans where we have already gotten the data
                continue
            # for now assume we have one scan per scan_id. This is a bad assumption
            # and should be fortified before too long
            try:
                hdr = db[scan]
            except ValueError:
                print("%s is not a valid scan" % scan)
            else:
                if scan not in data:
                    scalar_keys = set()
                    for descriptor in hdr['descriptors']:
                        for k, v in descriptor['data_keys'].items():
                            if 'external' not in v:
                                scalar_keys.add(k)
                    data[scan] = get_table(hdr, fill=fill_checkbox.value, fields=scalar_keys)
                valid_scans.add(scan)
        logger.debug('valid_scans = %s', valid_scans)
        if valid_scans:
            if key_select.value == 'Intersection':
                keys = set.intersection(*[set(d) for scan_id, d in data.items()])
            else:
                keys = set([key for scan_id, d in data.items() for key in list(d)])
        else:
            keys = []
        sorted_scans = sorted(list(valid_scans))
        sorted_keys = sorted(keys)
    #     scan_select.selected_labels = type(scan_select.selected_labels)()
    #     scan_select.selected_labels = []
        scan_select.options = sorted_scans
    #     x_dropdown.selected_label = sorted_keys[0]
        x_dropdown.options = sorted_keys
    #     y_select.selected_labels = []
        y_select.options = sorted_keys


    def get_scans(scans):
        print('scans = %s' % scans)
        if ',' in scans:
            scans = scans.split(',')
        else:
            scans = [scans]
        print('scans = %s' % scans)
        all_scans = set()

        splitter = {
            '-': _has_hyphen,
            ':': _has_colon,
        }
        for scan in scans:
            for delimiter, split in splitter.items():
                # parse the scans with delimiters
                if delimiter in scan:
                    all_scans.update(split(scan))
                    break
            else:
                # if no delimiter is present then assume it is just one scan
                try:
                    scan = int(scan)
                except ValueError:
                    raise ValueError("'%s' cannot be interpreted as an integer" % scan)
                # if no error, append the scan as an integer
                all_scans.add(scan)
        return all_scans


    def replot(sender):
        print('sender = %s' % sender)
        scans_to_plot = scan_select.value
        print('input scan value = {}'.format(scans_to_plot))
        x_name = x_dropdown.value
        y_names= y_select.value
        if not scans_to_plot or not x_name or not y_names:
            return
        ax.cla()
        for scan in scans_to_plot:
            for y_axis in y_names:
                try:
                    x_data = data[scan][x_name]
                    y_data = data[scan][y_axis]
                    label = '%s, %s' % (scan, y_axis)
                except KeyError:
                    x_data = []
                    y_data = []
                    label = '%s, %s. No data available' % (scan, y_axis)

                ax.plot(x_data, y_data, label=label)
        ax.legend(loc=0)
        ax.set_xlabel(x_name)

        plt.show()

        print('selected scans = {}'.format(scan_select.value))
    data = {}
    # create the widgets

    x_dropdown = Select(description="X axis")
    y_select = SelectMultiple(description='Y axes')
    scan_select = SelectMultiple(description="Select scans")
    fill_checkbox = Checkbox(description='Get file data (slow!)', disabled=True)
    key_select = ToggleButtons(description='Plotting keys',
                               options=['Intersection', 'All'])
    scan_text = Text(description="Scan IDs", options=['ab', 'c'])

    scan_text.on_submit(scan_submit)
    scan_select.on_trait_change(replot, 'value')
    x_dropdown.on_trait_change(replot, 'value')
    y_select.on_trait_change(replot, 'value')
    # recompute the plottable keys when intersection or all is computed
    key_select.on_trait_change(scan_submit, 'value')

    widgets = {'x_dropdown': x_dropdown,
               'y_select': y_select,
               'scan_select': scan_select,
               'fill_checkbox': fill_checkbox,
               'key_select': key_select,
               'scan_text': scan_text}
    box = VBox([fill_checkbox, HBox([scan_text, key_select]), scan_select, x_dropdown, y_select])
    return namedtuple('explorer_return_values', ['widgets', 'display'])(widgets, box)
