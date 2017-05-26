#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    "requests",
    "retrying",
    "nose"
]

setup(
    name='cook_integration',
    version='0.1.0',
    description="Integration tests for Cook scheduler",
    url='https://github.com/twosigma/Cook',
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='cook_integration',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=['nose>=1.0']
)
