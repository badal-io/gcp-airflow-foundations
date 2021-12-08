# Configuration file for the Sphinx documentation builder.

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
    "airflow": ("https://airflow.apache.org/", None),
}

intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'