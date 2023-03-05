#!/usr/bin/env python

from setuptools import setup
import re
import os
import sys

def get_packages(package):
    """Return root package and all sub-packages."""
    return [dirpath
            for dirpath, dirnames, filenames in os.walk(package)
            if os.path.exists(os.path.join(dirpath, '__init__.py'))]

setup(
    name="quality_mkdocs",
    version="0.1",
    license='APL2',
    packages=get_packages("quality_mkdocs"),
    install_requires=[
    ],
    python_requires='>=3.6',
    entry_points={
    },
    classifiers=[
    ],
    zip_safe=False
)
