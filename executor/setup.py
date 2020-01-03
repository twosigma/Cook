#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

test_deps=[
    'pytest==5.2.0',
    'pytest-timeout==1.3.3',
    'pytest-xdist==1.30.0'
]

extras = { 'test': test_deps }

setup(
    name='cook-executor',
    version=open("cook/_version.py").readlines()[-1].split('"')[1],
    description='Custom Mesos executor for Cook written in Python',
    url='https://github.com/twosigma/Cook',
    license="Apache Software License 2.0",
    keywords='cook-executor',
    packages=['cook'],
    test_suite='tests',
    tests_require=test_deps,
    extras_require=extras,
    install_requires=['psutil==5.4.1', 'pymesos==0.3.12'],
    entry_points={
        'console_scripts': [
            'cook-executor = cook.__main__:main'
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3.5"
    ]
)
