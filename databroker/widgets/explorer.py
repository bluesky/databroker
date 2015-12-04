from databroker import DataBroker as db, get_events, get_table
from ipywidgets import (interact, Text, Dropdown, Checkbox, VBox, HBox,
                        SelectMultiple, ToggleButtons, Select)
import matplotlib.pyplot as plt
from collections import namedtuple
from traitlets import Tuple, Unicode, HasTraits, Set, Int, observe, link


def explorer():

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

    splitter = {
        '-': _has_hyphen,
        ':': _has_colon,
    }

    def scan_submit(sender):
        all_scans = get_scans(scan_text.value)
        valid_scans = set()
        for scan in all_scans:
            if scan in data:
                # skip scans where we have already gotten the data
                continue
            # for now assume we have one scan per scan_id. This is a bad assumption
            # and should be fortified before too long
            try:
                hdr = db[scan]
            except ValueError:
                print("%s is not a valid scan" % scan)
                pass
            else:
                if scan not in data:
                    data[scan] = get_table(hdr, fill=fill_checkbox.value)
                valid_scans.add(scan)
        print('valid_scans = %s' % valid_scans)
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
        x = x_dropdown.value
        y = y_select.value
        if not scans_to_plot or not x or not y:
            return
        ax.cla()
        for scan in scans_to_plot:
            for y_axis in y:
                ax.plot(data[scan][x], data[scan][y_axis], label='%s, %s' % (scan, y_axis))
        ax.legend(loc=0)
        ax.set_xlabel(x)

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
    scan_select.observe(replot, 'value')
    x_dropdown.observe(replot, 'value')
    y_select.observe(replot, 'value')

    fig, ax = plt.subplots()
    widgets = {'x_dropdown': x_dropdown,
               'y_select': y_select,
               'scan_select': scan_select,
               'fill_checkbox': fill_checkbox,
               'key_select': key_select,
               'scan_text': scan_text}
    box = VBox([fill_checkbox, scan_text, scan_select, x_dropdown, y_select])
    return widgets, box
