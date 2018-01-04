# Cook integration tests


Integration tests for Cook scheduler

## Running in docker

If you're running cook in docker, it can be beneficial to run the integration tests in a docker container so that the IP addresses resolve correctly. To do so, there are two separate scripts:

```bash
$ bin/build-docker-image.sh
```

This needs to be run once, and can be run again in order to include any new dependencies in the docker image.

```bash
$ bin/run-integration.sh COOK_PORT_1 COOK_PORT_2
```

This launches the integration tests in the docker container. You can optionally pass one (or two, if using `COOK_MULTI_CLUSTER`) ports that cook is listening on. This mounts the local directory in the docker container so there is no need to rebuild when changing the tests.

## Running the tests locally

Running the integration tests from your local machine may simplify your development workflow.

### Installing dependencies

Our integration tests currently require Python 3.6.
All dependencies are specified in [`requirements_dev.txt`](./requirements_dev.txt),
and can be installed automatically via `pip`:

```bash
$ pip install -r requirements.txt
```

### Basic tests

To run the tests on a local cook install, run

```bash
$ pytest
```

To configure the Cook scheduler URL, set the `COOK_SCHEDULER_URL` environment variable. The default value is `http://localhost:12321`.

If you want to run a single test and see the log messages as they occur, you can run, for example:

```bash
$ pytest -n 0 --capture=no tests/cook/test_basic.py::CookTest::test_basic_submit
```

Or, more concisely:

```bash
$ pytest -n 0 -s -k test_basic_submit
```

However, note that invoking `pytest` with the `-k` option runs all tests functions that contain the given string,
meaning that more than one test may run if multiple tests have names matching the given string.

### Multi-scheduler tests

The [test_multi_cluster.py](tests/cook/test_multi_cluster.py) file contains integration tests that rely on two separate running schedulers. These tests are skipped by default, since we often only want to target a single cook cluster. If you want to include these tests, make sure to set the `COOK_MULTI_CLUSTER` environment variable, for example:
 
 ```bash
 $ COOK_MULTI_CLUSTER= pytest tests/cook/test_multi_cluster.py
 ```

To configure the second cook scheduler URL, set the `COOK_SCHEDULER_URL_2` environment variable. The default value is `http://localhost:22321`.

## Credits

This package was created with (Cookiecutter)[https://github.com/audreyr/cookiecutter] and the (audreyr/cookiecutter-pypackage)[https://github.com/audreyr/cookiecutter-pypackage] project template.
