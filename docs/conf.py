# Configuration file for the Sphinx documentation builder.
import sys
import os

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath(".."))

# For plugins that can not read conf.py.
# See also: https://github.com/docascode/sphinx-docfx-yaml/issues/85
sys.path.insert(0, os.path.abspath("."))

# -- Project information

project = 'GCP Airflow Foundations'
copyright = '2021, Badal.io'
author = 'Badal'

release = '0.2.6'
version = '0.2.6'

# -- General configuration

extensions = [
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    "sphinx.ext.napoleon",
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosectionlabel'
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/", None),
    "airflow": ("https://airflow.apache.org/", None)
}

intersphinx_disabled_domains = ['std']

autoclass_content = "both"
autodoc_default_options = {"members": True}
autosummary_generate = True  # Turn on sphinx.ext.autosummary

templates_path = ['_templates']

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True