# -*- coding: utf-8 -*-
"""
    sphinxext.xfig
    ~~~~~~~~~~~~~~

    Allow insert xfig files

    Adapted from sphix.ext.graphviz

    :copyright: Copyright 2015 by the BNL/BSA
    :license: BSD, see LICENSE for details.

"""

import re
import codecs
import posixpath
from os import path
from subprocess import Popen, PIPE
from hashlib import sha1

from six import text_type
from docutils import nodes
from docutils.parsers.rst import directives
from docutils.statemachine import ViewList

import sphinx
from sphinx.locale import _
from sphinx.util.osutil import ensuredir, ENOENT, EPIPE, EINVAL
from sphinx.util.compat import Directive


mapname_re = re.compile(r'<map id="(.*?)"')


class XfigError(Exception):
    pass


class xfig(nodes.General, nodes.Element):
    pass


def figure_wrapper(directive, node, caption):
    figure_node = nodes.figure('', node)

    parsed = nodes.Element()
    directive.state.nested_parse(ViewList([caption], source=''),
                                 directive.content_offset, parsed)
    caption_node = nodes.caption(parsed[0].rawsource, '',
                                 *parsed[0].children)
    caption_node.source = parsed[0].source
    caption_node.line = parsed[0].line
    figure_node += caption_node
    return figure_node


class XFig(Directive):
    """
    Directive to insert an xfig file
    """
    required_arguments = 1
    final_argument_whitespace = False
    option_spec = {
        'alt': directives.unchanged,
        'inline': directives.flag,
    }

    def run(self):
        document = self.state.document
        if self.content:
            return [document.reporter.warning(
                'Graphviz directive cannot have both content and '
                'a filename argument', line=self.lineno)]
        env = self.state.document.settings.env
        rel_filename, filename = env.relfn2path(self.arguments[0])
        env.note_dependency(rel_filename)

        node = xfig()
        node['rel_fpath'] = rel_filename
        node['fname'] = filename
        node['options'] = []

        return [node]


def convent_file(self, inp_fname, options, exp_format, prefix):
    hashkey = (inp_fname + str(options) +
              str(self.builder.config.xfig_fig2dev) +
              str(self.builder.config.xfig_fig2dev_args)
              ).encode('utf-8')

    fname = '%s-%s.%s' % (prefix, sha1(hashkey).hexdigest(), exp_format)
    print(fname)
    relfn = posixpath.join(self.builder.imgpath, fname)
    outfn = path.join(self.builder.outdir,
                      getattr(self.builder, 'imagedir', '_images'),
                      fname)

    if path.isfile(outfn):
        return relfn, outfn

    ensuredir(path.dirname(outfn))
    xfig_args = [self.builder.config.xfig_fig2dev]
    xfig_args = ['fig2dev']
    xfig_args.extend(['-L', exp_format])
    xfig_args.extend(self.builder.config.xfig_fig2dev_args)
    xfig_args.extend(options)
    xfig_args.append(inp_fname)
    xfig_args.append(outfn)
    print(xfig_args)
    try:
        p = Popen(xfig_args, stdout=PIPE, stdin=PIPE, stderr=PIPE)
        p.wait()
    except OSError as err:
        if err.errno != ENOENT:   # No such file or directory
            raise
        self.builder.warn('xfig command %r cannot be run (needed for xfig '
                          'output), check the xfig_fig2dev setting' %
                          self.builder.config.xfig_fig2dev)
        return None, None

    return relfn, outfn


def render_xfig_html(self, node, inp_fname, options, prefix='xfig',
                    imgcls=None, alt=None):
    format = self.builder.config.xfig_output_format
    try:

        fname, outfn = convent_file(self, inp_fname, options, format, prefix)
    except XfigError as exc:
        self.builder.warn('xfig code %r: ' % fname + str(exc))
        raise nodes.SkipNode

    inline = node.get('inline', False)
    if inline:
        wrapper = 'span'
    else:
        wrapper = 'p'

    self.body.append(self.starttag(node, wrapper, CLASS='xfig'))
    if fname is None:
        self.body.append(self.encode(inp_fname))
    else:
        if alt is None:
            alt = node.get('alt', self.encode(inp_fname).strip())
        imgcss = imgcls and 'class="%s"' % imgcls or ''
        if format == 'svg':
            svgtag = '<img src="%s" alt="%s" %s/>\n' % (fname, alt, imgcss)
            self.body.append(svgtag)
        else:
            self.body.append('<img src="%s" alt="%s" %s/>\n' %
                                 (fname, alt, imgcss))

    self.body.append('</%s>\n' % wrapper)
    raise nodes.SkipNode


def html_visit_xfig(self, node):
    render_xfig_html(self, node, node['fname'], node['options'])


def setup(app):
    app.add_node(xfig,
                 html=(html_visit_xfig, None))

    app.add_directive('xfig', XFig)
    app.add_config_value('xfig_output_format', 'svg', 'html')
    app.add_config_value('xfig_fig2dev', 'fig2dev', 'html')
    app.add_config_value('xfig_fig2dev_args', ['-m', '2'], 'html')
    return {'version': sphinx.__version__, 'parallel_read_safe': True}
