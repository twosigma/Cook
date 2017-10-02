#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup

test_requirements = [
    "requests",
    "retrying",
    "nose"
]

setup(
    name='cook_integration',
    version='0.1.0',
    description="Integration tests for Cook scheduler",
    include_package_data=True,
    install_requires=test_requirements,
    zip_safe=False,
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=['nose>=1.0']
)
