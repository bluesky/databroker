#
# based on
# https://github.com/spacetelescope/understanding-json-schema/blob/master/source/sphinxext/jsonschemaext.py
#
# Copyright (c) 2013, Space Telescope Science Institute
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
#   Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
#   Neither the name of the Space Telescope Science Institute nor the
#   names of its contributors may be used to endorse or promote products
#   derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

import json
import os
from docutils import nodes
from docutils import statemachine
from sphinx.util.compat import Directive
from sphinx.util.nodes import set_source_info
import difflib
import jsonschema
import jinja2


readble_template = jinja2.Template("""
<dl class='jsonschematable'>
{%- for item in jss recursive %}
   <dt class='jss_title h4'>{{ item.title }}</dt>
   <dd>

     <dl class='jsonschematable'>
       <dt class='jss_desc text-primary'>Description:</dt>
       <dd>{{ item.description }}</dd>
       <dt class='jss_type text-primary'>Type:</dt>
       <dd>{{ item.type }}</dd>
       {%- if item.properties -%}
       <dt class='jss_prop text-primary'>Properties:</dt><dd>
       <dl>
           {{ loop(item['properties'].values())|indent }}
       </dl>
       </dd>
       {%- endif -%}
     </dl>
   </dd>
{%- endfor %}
</dl>
""")


class jsonschema_node(nodes.Element):
    pass


class diff_node(nodes.Element):
    pass


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def pprint_json(jsdoc):
    """
    Pretty-print a json document

    Parameters
    ----------
    jsdoc : dict
        The json document to be pretty printed

    Returns
    -------
    pprint_str : str
        The json document 'pretty printed' as single string
        with '\\n'

    """
    return json.dumps(jsdoc, sort_keys=True, indent=4,
               separators=(',', ': '))


class _baseSchemaDirective(Directive):
    has_content = True
    validate = True

    def split_content(self, input_string):
        parts = []
        should_pass = True
        part = []
        comment = []

        def add_part():
            content = '\n'.join(part)
            if len(part) == 1:
                try:
                    rst_file = self.state_machine.document.attributes['source']
                    test_path = os.path.join(os.path.dirname(rst_file),
                                              content)
                    with open(test_path, 'r') as fin:
                        content = '\n'.join(fin)
                except FileNotFoundError:
                    pass
            try:
                json_content = json.loads(content)
            except ValueError:
                if should_pass:
                    raise ValueError("Invalid json: {0}".format(content))
                else:
                    # A complex number will never validate
                    json_content = 1+1j
            parts.append(AttrDict({
                'content': content,
                'json': json_content,
                'comment': comment}))

        for line in input_string:
            line = line.strip()

            if line.startswith('//'):
                line = line[2:].lstrip()
                if line:
                    comment.append(line)
            elif line == '--':
                add_part()
                part = []
                comment = []
            else:
                if line:
                    part.append(line)

        add_part()

        return parts


class SchemaDiffDirective(_baseSchemaDirective):

    def run(self):
        result = []

        parts = self.split_content(self.content)
        for part in parts:
            if len(part.comment):
                paragraph = nodes.paragraph('', '')
                comment = statemachine.StringList(part.comment)
                comment.parent = self.content.parent
                self.state.nested_parse(comment, 0, paragraph)
                paragraph['classes'] = ['jsonschema-comment']
                set_source_info(self, paragraph)
                result.append(paragraph)

            container = jsonschema_node()
            container['raw_json'] = part.json
            set_source_info(self, container)
            pprint_content = pprint_json(part.json)
            literal = nodes.literal_block(
                pprint_content, pprint_content)

            literal['language'] = 'json'
            set_source_info(self, literal)
            container.children.append(literal)
            result.append(container)

        for indx, part in enumerate(parts):
            for other_part in parts[(indx + 1):]:
                p1 = pprint_json(part.json).split('\n')
                p2 = pprint_json(other_part.json).split('\n')
                diff_str = '\n'.join(difflib.unified_diff(p2, p1,
                                     lineterm='',
                                     fromfile=(other_part.comment[0]
                                               if other_part.comment else ''),
                                     tofile=(part.comment[0]
                                             if part.comment else ''),))

                container = diff_node()
                set_source_info(self, container)
                literal = nodes.literal_block(
                    diff_str, diff_str)

                literal['language'] = 'diff'
                set_source_info(self, literal)
                container.children.append(literal)
                result.append(container)

        return result


def visit_jsonschema_node_html(self, node):
    pass


def _ensure_title(input_dict):
    return_dict = dict(input_dict)
    if 'properties' in input_dict:
        props = return_dict['properties']
        for k, v in props.items():
            if 'title' not in v:
                v['title'] = k
            if 'properties' in v:
                v['properties'] = _ensure_title[v]
    return return_dict


def depart_jsonschema_node_html(self, node):
    form_dict = _ensure_title(node['raw_json'])
    self.body.append(readble_template.render(
        jss=[form_dict]))


def visit_jsonschema_node_latex(self, node):
    adjust = False
    color = "gray"
    char = ""
    if 'jsonschema-pass' in node['classes']:
        char = r"\Checkmark"
        color = "ForestGreen"
        adjust = True
    elif 'jsonschema-fail' in node['classes']:
        char = r"\XSolidBrush"
        color = "BrickRed"
        adjust = True
    elif 'jsonschema' in node['classes']:
        char = r"\{ json schema \}"

    if adjust:
        self.body.append(r"\begin{adjustwidth}{2.5em}{0pt}")
    self.body.append(r"\begin{jsonframe}{%s}{%s}" % (char, color))


def depart_jsonschema_node_latex(self, node):
    adjust = False
    if 'jsonschema-pass' in node['classes']:
        adjust = True
    elif 'jsonschema-fail' in node['classes']:
        adjust = True

    self.body.append(r"\end{jsonframe}")
    if adjust:
        self.body.append(r"\end{adjustwidth}")


def visit_diff_node_html(self, node):
    pass


def depart_diff_node_html(self, node):
    pass


def visit_diff_node_latex(self, node):
    pass


def depart_diff_node_latex(self, node):
    pass


def setup(app):
    app.add_directive('schema_diff', SchemaDiffDirective)

    app.add_node(jsonschema_node,
                 html=(visit_jsonschema_node_html,
                       depart_jsonschema_node_html),
                 latex=(visit_jsonschema_node_latex,
                        depart_jsonschema_node_latex))
    app.add_node(diff_node,
                 html=(visit_diff_node_html, depart_diff_node_html),
                 latex=(visit_diff_node_latex, depart_diff_node_latex))


latex_preamble = r"""
\usepackage{changepage}
\usepackage[dvipsnames]{xcolor}
"""
