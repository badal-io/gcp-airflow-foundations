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
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/", None),
    "airflow": ("https://airflow.apache.org/", None)
}

intersphinx_disabled_domains = ['std']

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