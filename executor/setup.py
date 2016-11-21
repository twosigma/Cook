#!/usr/bin/env python3

from setuptools import setup

setup(name='cook',
      version='1.0',
      author="",
      author_email="",
      packages=['cook'],
      entry_points={
          'console_scripts': [
              'cook-executor = cook.__main__:main'
          ]
      }
)
