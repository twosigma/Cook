#!/usr/bin/env python3

from setuptools import setup

from cook.sidecar import version

requirements = [
    'flask~=1.1.0',
    'gunicorn~=20.1.0',
    'requests~=2.27.0',
]

test_requirements = [
]

setup(
    name='cook_sidecar',
    version=version.VERSION,
    description="Two Sigma's Cook Sidecar",
    long_description="The Cook Sidecar provides sandbox file access and progress reporting.",
    packages=['cook.sidecar'],
    entry_points={'console_scripts': ['cook-sidecar = cook.sidecar.__main__:main']},
    install_requires=requirements,
    tests_require=test_requirements
)
