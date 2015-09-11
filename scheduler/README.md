# Cook Scheduler

The Cook scheduler is a Mesos framework that is able to scheduler jobs submitted to it via its REST API from multiple users.
It's raison d'etre is to prioritize the user experience on a shared cluster.
It does so by allowing users to use as much resources as possible, preempting users with greater shares when new users show up, and allowing users to have new jobs jump to the head of their queue.
You can learn more about the algorithms used by Cook in this talk by Li Jin at MesosCon: https://www.youtube.com/watch?v=BkBMYUe76oI.

## Building

To build the cook scheduler, you'll need to run `lein voom build-deps` before it will build successfully.

In order to be able to use voom, please include the following in your project.clj:

```clojure
{:user {
    :plugins [[lein-voom "0.1.0-20150822_000839-g763d315"]]
  }
}
```

Voom is a tool that allows us to depend on git repositories at specific commits, rather than waiting for projects to release their code to a Maven repository.
We're working to remove the voom dependencies, or to simplify building them.

You can build the final jar correctly by running `lein release-jar`.

## Running

You can run the scheduler using the build system by running `lein run $config`, where config is an `.edn` file.
See `example-config.edn` for example usage.
You should be able to start a sample instance (without any persistence) using the `example-config`.
You'll need to have the environment variable `MESOS_NATIVE_JAVA_LIBRARY` set.

To deploy the scheduler, simply build a jar as described above, and then run it with `java -cp target/cook-0.1.0-SNAPSHOT-standalone.jar cook.components example-config.edn`.

See the `docs` folder for the API description.

© Two Sigma Open Source, LLC
