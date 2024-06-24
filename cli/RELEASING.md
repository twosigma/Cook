Releasing Cook Executor
=======================

Cook CLI is released on [PyPI](https://pypi.org/project/cook-executor/)

Prerequisites
-------------
Ensure you can build the cli following the instructions in README.md

Install `twine`:
```bash
pip3 install twine
```

Update version number
------------
Look at setup.py, and edit the version number which may reference another python variable to be edited.

Test Release
------------
Since PyPI does not allow modifying releases, it can be useful to test a release using their test instance.
```bash
rm -rf dist/*
python3 setup.py sdist bdist_wheel
python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```
Then, in a separate virtualenv for testing:
```bash
pip3 install  --index-url https://test.pypi.org/simple/ --no-deps cook-executor==$VERSION
pip3 install # install any other required dependencies from the main pypi repo
```
If there is an issue with the release, you can just release another version. They are GC-ed periodically from the test instance.

Production Release
------------------
When you're ready to release the final version, just build and upload to the standard PyPI repo.
```bash
rm -rf dist/*
python3 setup.py sdist bdist_wheel
python3 -m twine upload dist/*
```
