# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
import requests

import bluesky_tiled_plugins

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# General information about the project.
project = "bluesky-tiled-plugins"
copyright = "2025, Brookhaven National Lab"

# The full version, including alpha/beta/rc tags.
version = bluesky_tiled_plugins.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    # for diagrams
    "sphinxcontrib.mermaid",
    # For linking to external sphinx documentation
    "sphinx.ext.intersphinx",
    # Add links to source code in API docs
    "sphinx.ext.viewcode",
    # Add a copy button to each code block
    "sphinx_copybutton",
    # For the card element
    "sphinx_design",
    # To make .nojekyll
    "sphinx.ext.githubpages",
    # To make the {ipython} directive
    "IPython.sphinxext.ipython_directive",
    # To syntax highlight "ipython" language code blocks
    "IPython.sphinxext.ipython_console_highlighting",
    # To embed matplotlib plots generated from code
    "matplotlib.sphinxext.plot_directive",
    # To parse markdown
    "myst_parser",
]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# These patterns also affect html_static_path and html_extra_path
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Output graphviz directive produced images in a scalable format
graphviz_output_format = "svg"

# # The name of a reST role (builtin or Sphinx extension) to use as the default
# # role, that is, for text marked up `like this`
# default_role = "any"

# The suffix of source filenames.
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = "pydata_sphinx_theme"
github_repo = "databroker"
github_user = "bluesky"
switcher_json = "https://blueskyproject.io/bluesky-tiled-plugins/switcher.json"
switcher_exists = requests.get(switcher_json).ok
if not switcher_exists:
    print(
        "*** Can't read version switcher, is GitHub pages enabled? \n"
        "    Once Docs CI job has successfully run once, set the "
        "Github pages source branch to be 'gh-pages' at:\n"
        f"    https://github.com/{github_user}/{github_repo}/settings/pages",
        file=sys.stderr,
    )

# Theme options for pydata_sphinx_theme
# We don't check switcher because there are 3 possible states for a repo:
# 1. New project, docs are not published so there is no switcher
# 2. Existing project with latest skeleton, switcher exists and works
# 3. Existing project with old skeleton that makes broken switcher,
#    switcher exists but is broken
# Point 3 makes checking switcher difficult, because the updated skeleton
# will fix the switcher at the end of the docs workflow, but never gets a chance
# to complete as the docs build warns and fails.

html_theme_options = {
    "use_edit_page_button": True,
    "github_url": f"https://github.com/{github_user}/{github_repo}/tree/main/{project}",
    "icon_links": [
        {
            "name": "PyPI",
            "url": f"https://pypi.org/project/{project}",
            "icon": "fas fa-cube",
        },
    ],
    "switcher": {
        "json_url": switcher_json,
        "version_match": version,
    },
    "check_switcher": False,
    "navbar_end": ["theme-switcher", "icon-links", "version-switcher"],
    "external_links": [
        {
            "name": "Bluesky",
            "url": "https://blueskyproject.io/bluesky/main/index.html",
        },
        {
            "name": "Tiled",
            "url": "https://blueskyproject.io/tiled/",
        },
    ],
    "navigation_with_keys": False,
}

# A dictionary of values to pass into the template engine’s context for all pages
html_context = {
    "github_user": github_user,
    "github_repo": project,
    "github_version": version,
    "doc_path": "docs",
}

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
html_show_copyright = False

# # Logo
# html_logo = "images/ophyd-async-logo.svg"
# html_favicon = "images/ophyd-favicon.svg"

# Custom CSS
html_static_path = ["_static"]
html_css_files = ["custom.css"]

# Where to put Ipython savefigs
ipython_savefig_dir = "../_build/savefig"
