#!/usr/bin/env python3

from setuptools import setup

setup(
    name='cook',
    version='1.0',
    packages=['cook'],
    entry_points={
        'console_scripts': [
            'cs = cook.__main__:main'
        ]
    }
)
