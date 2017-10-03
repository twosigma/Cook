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
    name='cook_client',
    version='0.1.0a4',
    description="Two Sigma's Cook CLI",
    long_description="This package contains Two Sigma's Cook Scheduler command line interface, cs. cs allows you to "
                     "submit jobs and view jobs, job instances, and job groups across multiple Cook clusters.",
    packages=['cook', 'cook.subcommands'],
    entry_points={'console_scripts': ['cs = cook.__main__:main']},
    install_requires=requirements,
    tests_require=test_requirements
)
