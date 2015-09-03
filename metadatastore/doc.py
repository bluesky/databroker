from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
import collections
from functools import reduce

_HTML_TEMPLATE = """
<table>
{% for key, value in document | dictsort recursive %}
  <tr>
    <th> {{ key }} </th>
    <td>
      {% if value.items %}
        <table>
          {{ loop(value | dictsort) }}
        </table>
        {% else %}
          {% if key == 'time' %}
            {{ value | human_time }}
          {% else %}
            {{ value }}
          {% endif %}
        {% endif %}
    </td>
  </tr>
{% endfor %}
</table>
"""


class Document(dict):

    def __init__(self, name, *args, **kwargs):
        self._name = name
        super(Document, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError()

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError:
            raise AttributeError()

    def __iter__(self):
        return (k for k in super(Document, self).__iter__()
                if not (isinstance(k, six.string_types) and k.startswith('_')))

    def items(self):
        return ((k, v) for k, v in super(Document, self).items()
                if not (isinstance(k, six.string_types) and k.startswith('_')))

    def values(self):
        return (v for k, v in super(Document, self).items()
                if not (isinstance(k, six.string_types) and k.startswith('_')))

    def keys(self):
        return (k for k in super(Document, self).keys()
                if not (isinstance(k, six.string_types) and k.startswith('_')))

    def __len__(self):
        return len(list(self.keys()))

    def _repr_html_(self):
        import jinja2
        env = jinja2.Environment()
        env.filters['human_time'] = pretty_print_time
        template = env.from_string(_HTML_TEMPLATE)
        return template.render(document=self)

    def __str__(self):
        try:
            return vstr(self)
        except ImportError:
            return super(self, Document).__str__()


def pretty_print_time(timestamp):
    import humanize
    import time
    import datetime
    dt = datetime.datetime.fromtimestamp(timestamp).isoformat()
    ago = humanize.naturaltime(time.time() - timestamp)
    return '{ago} ({date})'.format(ago=ago, date=dt)


def _format_dict(value, name_width, value_width, name, tabs=0):
    ret = ''
    for k, v in six.iteritems(value):
        if isinstance(v, collections.Mapping):
            ret += _format_dict(v, name_width, value_width, k, tabs=tabs+1)
        else:
            ret += ("\n%s%-{}s: %-{}s".format(
                name_width, value_width) % ('  '*tabs, k[:16], v))
    return ret


def _format_data_keys_dict(data_keys_dict):
    from prettytable import PrettyTable

    fields = reduce(set.union,
                    (set(v) for v in six.itervalues(data_keys_dict)))
    fields = sorted(list(fields))
    table = PrettyTable(["data keys"] + list(fields))
    table.align["data keys"] = 'l'
    table.padding_width = 1
    for data_key, key_dict in sorted(data_keys_dict.items()):
        row = [data_key]
        for fld in fields:
            row.append(key_dict.get(fld, ''))
        table.add_row(row)
    return table


def vstr(doc, indent=0):
    """Recursive document walker and formatter

    Parameters
    ----------_
    name : str, optional
        Document header name. Defaults to ``doc._name``
    indent : int, optional
        The indentation level. Defaults to starting at 0 and adding one tab
        per recursion level
    """
    headings = [
        # characters recommended as headers by ReST docs
        '=', '-', '`', ':', '.', "'", '"', '~', '^', '_', '*', '+', '#',
        # all other valid header characters according to ReST docs
        '!', '$', '%', '&', '(', ')', ',', '/', ';', '<', '>', '?', '@',
        '[', '\\', ']', '{', '|', '}'
    ]
    name = doc['_name']

    ret = "\n%s\n%s" % (name, headings[indent]*len(name))

    documents = []
    name_width = 16
    value_width = 40
    for name, value in sorted(doc.items()):
        if name == 'descriptors':
            # this case is to deal with Headers from databroker
            for val in value:
                documents.append((name, val))
        elif name == 'data_keys':
            ret += "\n%s" % str(_format_data_keys_dict(value))
        elif isinstance(value, collections.Mapping):
            if '_name' in value:
                documents.append((name, value))
            else:
                # format dicts reasonably
                ret += "\n%-{}s:".format(name_width, value_width) % (name)
                ret += _format_dict(value, name_width, value_width,
                                    name, tabs=1)
        else:
            ret += ("\n%-{}s: %-{}s".format(name_width, value_width) %
                    (name[:16], value))
    for name, value in documents:
        ret += "\n%s" % (vstr(value, indent+1))
        # ret += "\n"
    ret = ret.split('\n')
    ret = ["%s%s" % ('  '*indent, line) for line in ret]
    ret = "\n".join(ret)
    return ret
