from __future__ import (absolute_import, division, print_function)
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


class DocumentIsReadOnly(Exception):
    pass


class Document(dict):

    def __init__(self, name, *args, **kwargs):
        super(Document, self).__init__(*args, **kwargs)
        super(Document, self).__setitem__('_name', name)
        super(Document, self).__setattr__('__dict__',  self)

    def __setattr__(self, key, value):
        raise DocumentIsReadOnly()

    def __setitem__(self, key, value):
        raise DocumentIsReadOnly()

    def __delattr__(self, key):
        raise DocumentIsReadOnly()

    def __delitem__(self, key):
        raise DocumentIsReadOnly()

    def update(self, *args, **kwargs):
        raise DocumentIsReadOnly()

    def pop(self, key):
        raise DocumentIsReadOnly()

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
            return super(Document, self).__str__()

    def to_name_dict_pair(self):
        """Convert to (name, dict) pair

        This can be used to safely mutate a Document::

           name, dd = doc.to_name_dict_pair()
           dd['new_key'] = 'aardvark'
           dd['run_start'] = dd['run_start']['uid']
           new_doc = Document(name, dd)

        Returns
        -------
        name : str
            Name of Document

        ret : dict
            Data payload of Document
        """
        ret = dict(self)
        name = ret.pop('_name')
        return name, ret


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
    doc : Document
        Dict-like thing to format, must have `_name` key
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


def ref_doc_to_uid(doc, field):
    """Convert a reference doc to a uid

    Given a Document, replace the given field (which must contain a
    Document) with the uid of that Document.

    Returns a new instance with the updated values

    Parameters
    ----------
    doc : Document
        The document to replace an entry in

    field : str
        The field to replace with the uid of it's contents
    """
    name, doc = doc.to_name_dict_pair()
    doc[field] = doc[field]['uid']
    return Document(name, doc)
