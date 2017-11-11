import os

import setuptools


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setuptools.setup(
    name="scrapenhl2",
    version="0.4.00",
    author="Muneeb Alam",
    author_email="muneeb.alam@gmail.com",
    description=("scrapenhl2 is a python package for scraping and manipulating NHL data pulled from the NHL website."),
    license="MIT",
    keywords="nhl",
    url="https://github.com/muneebalam/scrapenhl2",
    packages=setuptools.find_packages(),
    install_requires=['numpy',  # used by pandas
                      'scipy',  # not currently used, but may be used for distribution fitting
                      'matplotlib',  # graphing
                      'seaborn',  # graphing; a little nicer than MPL
                      'pandas',  # for handling and manipulating data
                      'pyarrow',  # used by feather
                      'feather-format',  # fast read-write format that plays nicely with R
                      'halo',  # for spinners
                      'scikit-learn',  # not currently used, but will be for machine learning
                      'flask',  # for front end
                      'python-Levenshtein',  # for fast fuzzy matching
                      'fuzzywuzzy',  # for fuzzy string matching
                      'beautifulsoup4==4.5.3',  # for html parsing
                      'html-table-extractor',  # for html parsing
                      'plotly',  # for interactive charts
                      'tables',
                      'dash',
                      'dash-renderer',
                      'dash-html-components',
                      'dash-core-components'
                      ],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        'Intended Audience :: Science/Research',
        "License :: OSI Approved :: MIT License",
        'Programming Language :: Python :: 3'
    ],

)
