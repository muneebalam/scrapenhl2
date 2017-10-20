import os

import setuptools


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setuptools.setup(
    name = "scrapenhl2",
    version="0.0.202",
    author = "Muneeb Alam",
    author_email = "muneeb.alam@gmail.com",
    description = ("scrapenhl2 is a python package for scraping and manipulating NHL data pulled from the NHL website."),
    license = "MIT",
    keywords = "nhl",
    url = "https://github.com/muneebalam/scrapenhl2",
    packages=['scrapenhl2', 'docs', 'tests'],
    install_requires=['numpy', 'scipy', 'matplotlib', 'seaborn', 'pandas', 'feather-format', 'halo', 'pyarrow',
                      'scikit-learn', 'flask', 'setuptools'],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        'Intended Audience :: Science/Research',
        "License :: OSI Approved :: MIT License",
        'Programming Language :: Python :: 3'
    ],

)