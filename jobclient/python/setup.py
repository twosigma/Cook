#!/usr/bin/env python3

from setuptools import setup

from cook.scheduler.client import CLIENT_VERSION

with open('README.md') as fd:
    readme = fd.read()

requirements = [
    'requests'
]

setup(name='cook-scheduler-client',
      version=CLIENT_VERSION,
      description="Cook Scheduler Client API for Python",
      long_description=readme,
      long_description_content_type='text/markdown',
      packages=['cook.scheduler.client'],
      url='https://github.com/twosigma/Cook',
      install_requires=requirements,
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: OS Independent"
      ],
      python_requires='>=3.6')
