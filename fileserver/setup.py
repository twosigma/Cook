#!/usr/bin/env python3

from setuptools import setup

from cook import version

requirements = [
    'Flask==1.1.0',
    'gunicorn==19.9.0'
]

test_requirements = [
]

setup(
    name='cook_file_server',
    version=version.VERSION,
    description="Two Sigma's Cook File Server",
    long_description="This package contains Two Sigma's Cook file server. The primary purpose is run in a side car "
                     "on a job's node and to serve log files.",
    packages=['cook'],
    entry_points={'console_scripts': ['fileserver = cook.__main__:main']},
    install_requires=requirements,
    tests_require=test_requirements
)
