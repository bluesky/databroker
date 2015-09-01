from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

_HTML_TEMPLATE = """
<table>
{% for key, value in document.items() recursive %}
  <tr>
    <th> {{ key }} </th>
    <td>
      {% if value.items %}
        <table>
          {{ loop(value.items()) }}
        </table>
        {% else %}
          {{ value }}
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
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        del self[key]

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

    def __contains__(self, k):
        if isinstance(k, six.string_types) and k.startswith('_'):
            return False
        return super(Document, self).__contains(k)

    def _repr_html_(self):
        import jinja2
        return jinja2.Template(_HTML_TEMPLATE).render(document=self)
