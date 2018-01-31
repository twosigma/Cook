# Integration tests for Cook Scheduler

Our integration tests currently require Python 3.6.
All dependencies are specified in [`requirements.txt`](./requirements.txt),
and can be installed automatically via `pip`:

```bash
$ pip install -r requirements.txt
```

To run the tests on a local cook install, run

```bash
$ pytest
```

To configure the Cook scheduler URL, set the `COOK_SCHEDULER_URL` environment variable. The default value is `http://localhost:12321`.

If you want to run a single test and see the log messages as they occur, you can run, for example:

```bash
$ ./bin/only-run test_basic_submit
```

The `./bin/only-run` helper script invokes pytest internally.
The above command is roughly equivalent to the following pytest invocation:

```bash
$ pytest -v -n0 --capture=no tests/cook/test_basic.py::CookTest::test_basic_submit
```

## Multi-scheduler tests

The [test_multi_cluster.py](tests/cook/test_multi_cluster.py) file contains integration tests that rely on two separate running schedulers.
These tests are skipped by default, since we often only want to target a single cook cluster.
If you want to include these tests, make sure to set the `COOK_MULTI_CLUSTER` environment variable, for example:
 
 ```bash
 $ COOK_MULTI_CLUSTER="" pytest tests/cook/test_multi_cluster.py
 ```

To configure the second cook scheduler URL, set the `COOK_SCHEDULER_URL_2` environment variable. The default value is `http://localhost:22321`.

## Running in docker

If you're running cook in docker, it can be beneficial to run the integration tests in a docker container so that the IP addresses resolve correctly. To do so, there are two separate scripts:

```bash
$ bin/build-docker-image.sh
```

This needs to be run once, and can be run again in order to include any new dependencies in the docker image.

```bash
$ bin/run-integration.sh
```

This runs the integration tests in the docker container.
You can optionally pass one (or two, if using `COOK_MULTI_CLUSTER`) ports that cook is listening on:

```bash
$ bin/run-integration.sh --port 12321 --port-2 22321
```

By default, this script mounts the local directory in the docker container so there is no need to rebuild when changing the tests.
You can disable this behavior using the `--no-mount` option.

## Running multi-user tests with Kerberos

Running the multi-user integration tests with Kerberos requires that the test framework
has a way to get valid Kerberos authentication tickets for the many users from the tests.
We use several environment variables to make this possible.

* `COOK_TEST_USER_PREFIX`:
The numbers 0-9 will be appended to this username prefix to generate usernames for the tests.
We currently assume that no Kerberos-enabled test will need more than 10 unique test users.

* `COOK_ADMIN_USER_NAME`:
The username to be used in tests specifically requiring an admin user.
This user should *not* have impersonation priviledges.

* `COOK_IMPERSONATOR_USER_NAME`:
The username to be used in tests specifically requiring an impersonator user.
This user should *not* have administrator priviledges.

* `COOK_KERBEROS_TEST_AUTH_CMD`:
Command string (run in a shell subprocess) to obtain a test user's Kerberos ticket.
The command can contain the special strings `{{COOK_USER}}`,
which will be replaced with the target test user's name,
and `{{COOK_SCHEDULER_URL}}`,
which will be replaced with the Cook Scheduler URL being used by the current tests.
The command must print (to `stdout`) the `Authorization` header value for the target user.

* `COOK_MAX_TEST_USERS`:
Setting this environment variable limits the number of test users allocated to each test runner process.
This is most likely necessary in any scenario where users are actually authenticated by the underlying OS.

## Credits

This package was created with (Cookiecutter)[https://github.com/audreyr/cookiecutter] and the (audreyr/cookiecutter-pypackage)[https://github.com/audreyr/cookiecutter-pypackage] project template.
