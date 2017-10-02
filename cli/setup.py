#!/usr/bin/env python3

from setuptools import setup

requirements = [
    'arrow',
    'blessings',
    'humanfriendly',
    'requests',
    'tabulate'
]

test_requirements = [
    'nose',
    'requests-mock'
]

setup(
    name='twosigma.cook-cli',
    version='0.1.0a1',
    packages=['cook', 'cook.subcommands'],
    entry_points={'console_scripts': ['cs = cook.__main__:main']},
    install_requires=requirements,
    tests_require=test_requirements
)
