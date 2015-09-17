# Cook Scheduler

The Cook scheduler is a Mesos framework that is able to scheduler jobs submitted to it via its REST API from multiple users.
It's raison d'etre is to prioritize the user experience on a shared cluster.
It does so by allowing users to use as much resources as possible, preempting users with greater shares when new users show up, and allowing users to have new jobs jump to the head of their queue.
You can learn more about the algorithms used by Cook in this talk by Li Jin at MesosCon: https://www.youtube.com/watch?v=BkBMYUe76oI.

## Building

You can build the final jar by running `lein uberjar`.

## Running

You can run the scheduler using the build system by running `lein run $config`, where config is an `.edn` file.
See `example-config.edn` for example usage.
You should be able to start a sample instance (without any persistence) using the `example-config`.
You'll need to have the environment variable `MESOS_NATIVE_JAVA_LIBRARY` set.

To deploy the scheduler, simply build a jar as described above, and then run it with `java -cp target/cook-0.1.0-SNAPSHOT-standalone.jar cook.components example-config.edn`.
See the Asciidoc document `docs/configuration.asc` for details on the config file format and options.

## API

Cook comes with two APIs: the REST API, and the Datomic API.
To use the REST API, see the `docs/scheduler-rest-api.asc` Asciidoc document for the API description.
Most users should use the REST API, since it's compatible and will not change.

For advanced integration with JVM-based projects, you can write new jobs directly to Datomic, and Cook will automatically detect these jobs and scheduler them.
To use this API, you should already be familiar with (Datomic)[docs.datomic.com].
You can see how to create a new job and query it by looking at the implementation of the REST API in `src/cook/mesos/api.clj`.
Job creation is done in the `submit-jobs` function, and job querying is done in `fetch-job-map`.
The direct Datomic access is especially useful for computing analytics on the Cook scheduler's behavior.
Because of Datomic's architecture, you can spin up a Datomic peer and freely do heavy full-db scans and analytics on the production database without impacting the performance of the Cook server.

## Dependencies

Cook is written in Clojure.
To develop Cook, all you need is a JVM and Mesos installed and configured.
Cook will automatically start embedded copies of the rest of its dependencies.

When deploying Cook in production, you'll also want to provide a few other dependencies:

- Zookeeper is used for leader election of the Cook schedulers, so that it can support high availability. Cook puts almost zero load on Zookeeper, so feel free to use a shared Zookeeper.
- Datomic is used to store all data.

You'll need to deploy [Datomic](http://www.datomic.com/pricing.html) as well.
By default, you can use the Free edition of Datomic, which runs on a single machine and writes all its data locally.
Be sure to make backups!

If you're running Cook in an enviroment with higher availability requirements, you can use Datomic Pro or Pro Starter.
To use these, you'll need to install Datomic Pro locally, and then change the dependency in `project.clj` from `com.datomic/datomic-free` to `com.datomic/datomic-pro`.
We recommend not changing the version of Datomic, although moving to a version shouldn't cause any issues.

If you'd like email notifications on critical exceptions, make sure that JavaMail will work on the server you've set up.

© Two Sigma Open Source, LLC
