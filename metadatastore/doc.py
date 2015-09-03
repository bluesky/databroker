from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

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

    def __len__(self):
        return len(list(self.keys()))

    def __contains__(self, k):
        if isinstance(k, six.string_types) and k.startswith('_'):
            return False
        return super(Document, self).__contains__(k)

    def _repr_html_(self):
        import jinja2
        env = jinja2.Environment()
        env.filters['human_time'] = pretty_print_time
        template = env.from_string(_HTML_TEMPLATE)
        return template.render(document=self)


def pretty_print_time(timestamp):
    import humanize
    import time
    import datetime
    dt = datetime.datetime.fromtimestamp(timestamp).isoformat()
    ago = humanize.naturaltime(time.time() - timestamp)
    return '{ago} ({date})'.format(ago=ago, date=dt)
