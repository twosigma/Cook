#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name='cook-executor',
    version=open("cook/_version.py").readlines()[-1].split('"')[1],
    description='Custom Mesos executor for Cook written in Python',
    url='https://github.com/twosigma/Cook',
    license="Apache Software License 2.0",
    keywords='cook-executor',
    packages=['cook'],
    test_suite='tests',
    tests_require=[
        'nose>=1.0'
    ],
    setup_requires=['nose>=1.0'],
    install_requires=['pymesos==0.2.15'],
    entry_points={
        'console_scripts': [
            'cook-executor = cook.__main__:main'
        ]
    }
)
