Requirements Parser
===================

[![Python CI](https://github.com/madpah/requirements-parser/actions/workflows/poetry.yml/badge.svg)](https://github.com/madpah/requirements-parser/actions/workflows/poetry.yml)
[![Documentation Status](http://readthedocs.org/projects/requirements-parser/badge/?version=latest)](http://requirements-parser.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This is a small Python module for parsing [Pip](http://www.pip-installer.org/) requirement files.

The goal is to parse everything in the 
[Pip requirement file format](https://pip.pypa.io/en/stable/reference/pip_install/#requirements-file-format) spec.

Installation
============

    pip install requirements-parser

or

    poetry add requirements-parser

Examples
========

Requirements parser can parse a file-like object or a text string.

``` {.python}
>>> import requirements
>>> with open('requirements.txt', 'r') as fd:
...     for req in requirements.parse(fd):
...         print(req.name, req.specs)
Django [('>=', '1.11'), ('<', '1.12')]
six [('==', '1.10.0')]
```

It can handle most if not all of the options in requirement files that
do not involve traversing the local filesystem. These include:

-   editables (`-e git+https://github.com/toastdriven/pyelasticsearch.git]{.title-ref}`)
-   version control URIs
-   egg hashes and subdirectories (`[\#egg=django-haystack&subdirectory=setup]{.title-ref}`)
-   extras ([DocParser\[PDF\]]{.title-ref})
-   URLs

Documentation
=============

For more details and examples, the documentation is available at:
<http://requirements-parser.readthedocs.io>.


Change Log
==========

Change log is available on GitHub [here]()
