# Cook integration tests


Integration tests for Cook scheduler

To run the tests on a local cook install, run

```bash
$ python setup.py nosetests
```

To configure the Cook scheduler URL, set the `COOK_SCHEDULER_URL` environment variable. The default value is `http://localhost:12321`.

If you want to run a single test and see the log messages as they occur, you can run, for example:

```bash
$ nosetests tests.cook.test_basic:CookTest.test_max_runtime_exceeded --nologcapture
```

## Credits

This package was created with (Cookiecutter)[https://github.com/audreyr/cookiecutter] and the (audreyr/cookiecutter-pypackage)[https://github.com/audreyr/cookiecutter-pypackage] project template.
