from os.path import dirname, realpath, exists
from setuptools import setup, find_packages
import sys


author = u"Paul Müller"
authors = [author]
description = 'GUI for managing data on DCOR'
name = 'dcoraid'
year = "2019"

sys.path.insert(0, realpath(dirname(__file__))+"/"+name)
from _version import version  # noqa: E402

setup(
    name=name,
    author=author,
    author_email='dev@craban.de',
    url='https://github.com/DCOR-dev/DCOR-Aid',
    version=version,
    packages=find_packages(),
    package_dir={name: name},
    include_package_data=True,
    license="GPL v3",
    description=description,
    long_description=open('README.rst').read() if exists('README.rst') else '',
    install_requires=["appdirs",  # cache directory for data compression
                      "dclab[dcor]==0.34.6",  # pin for triage
                      "pyqt5",
                      "requests>=2.13",
                      "requests_toolbelt",  # multipart uploads with progress
                      ],
    python_requires='>=3.8, <4',
    entry_points={"gui_scripts": ['dcoraid = dcoraid.__main__:main']},
    keywords=["RT-DC", "deformability", "cytometry", "zellmechanik"],
    classifiers=['Operating System :: OS Independent',
                 'Programming Language :: Python :: 3',
                 'Intended Audience :: Science/Research',
                 ],
    platforms=['ALL']
    )
