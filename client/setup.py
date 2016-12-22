#!/usr/bin/env python3

from setuptools import setup
from setuptools.command.test import test as TestCommand

class PyTestCommand(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # Import here, because outside the eggs aren't loaded
        import pytest
        pytest.main(self.test_args)

setup(
    name='cook',
    version='1.0',
    author="",
    author_email="",
    packages=['cook'],
    cmdclass={'test': PyTestCommand},
    tests_require=[
        'pytest>=3.0.2'
    ],
    install_requires=[
        'bravado>=8.4.0'
    ],
    entry_points={
        'console_scripts': [
            'cook = cook.__main__:cli'
        ]
    }
)
