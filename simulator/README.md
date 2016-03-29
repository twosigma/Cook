# Cook Simulator

This project is a CLI interface to generated simulated workloads, to run them against a real instance of Cook, and to report on how the jobs in a given simulation were handled.

## Example usage

First, edit your intended settings file (e.g. config/settings.edn) as you see fit.
Since the program exits after each command, you'll need to be running against a
Datomic with persistent storage.

<pre><code>
~/projects/cook/simulator $ lein run -c config/settings.edn setup-database
~/projects/cook/simulator $ lein run -c config/settings.edn generate-job-schedule
Loading settings from  config/settings.edn
Connecting to simulation database...
generating job schedule...
New job schedule created:  382630046468258
To run a simulation using this schedule, try arguments simulate -e  382630046468258
~/projects/cook/simulator $ lein run -c config/settings.edn list-job-schedules
Loading settings from  config/settings.edn
Connecting to simulation database...
Listing available job schedules...

|              ID | Duration | Num Researchers |                      Created |
|-----------------+----------+-----------------+------------------------------|
| 382630046467072 |  10 secs |               1 | Thu Mar 10 11:00:43 EST 2016 |
| 382630046467447 |  10 secs |               1 | Thu Mar 10 15:59:50 EST 2016 |
| 382630046467510 |  10 secs |               1 | Thu Mar 10 16:13:56 EST 2016 |
| 382630046467516 |  10 secs |               1 | Thu Mar 10 16:25:43 EST 2016 |
| 382630046467861 |  10 secs |               1 | Fri Mar 11 06:55:00 EST 2016 |
| 382630046467872 |  10 secs |               1 | Fri Mar 11 06:56:15 EST 2016 |
| 382630046467883 |  10 secs |               1 | Fri Mar 11 07:00:10 EST 2016 |
| 382630046468258 |  10 secs |               1 | Fri Mar 11 09:59:16 EST 2016 |
~/projects/cook/simulator $ lein run -c config/settings.edn simulate -e  382630046468258
Loading settings from  config/settings.edn
Connecting to simulation database...
Running a simulation for schedule  382630046468258 ...
Created simulation  387028092979371 .
After jobs are finished, try running report -e  387028092979371 .
scheduling cook job with payload:  {"jobs":[{"max_retries":3,"max_runtime":86400000,"mem":602,"cpus":2.0,"uuid":"56e2dd81-dc74-4715-999c-16b66248239b","command":"sleep 4; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"max_retries":3,"max_runtime":86400000,"mem":511,"cpus":2.0,"uuid":"56e2dd82-3fe5-4fc4-b9a8-f3f9a7f8cd3c","command":"sleep 1; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"max_retries":3,"max_runtime":86400000,"mem":44,"cpus":2.0,"uuid":"56e2dd84-9c57-4163-82b3-f8dc6a40dc5d","command":"sleep 3; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"max_retries":3,"max_runtime":86400000,"mem":885,"cpus":1.0,"uuid":"56e2dd85-bb26-492a-bef0-b4aa93f18adb","command":"sleep 10; exit 0"}]}
"Elapsed time: 5593.977295 msecs"
~/projects/cook/simulator $ lein run -c config/settings.edn list-sims -e  382630046468258
Loading settings from  config/settings.edn
Connecting to simulation database...
listing simulation runs for test 382630046468258 ...

|              ID |                         When |
|-----------------+------------------------------|
| 387028092979371 | Fri Mar 11 10:00:15 EST 2016 |
~/projects/cook/simulator $ lein run -c config/settings.edn report  -e  387028092979371
Loading settings from  config/settings.edn
Connecting to simulation database...
Analyzing sim  387028092979371 ...
Average wait time for jobs is 6.278 seconds.
~/projects/cook/simulator $

</code></pre>

## TODOS:

* Make workloads more realistic.  Currently it's just a certain number of researchers,
all scheduling jobs with geometrically distributed duration and CPU and memory requirements (the same settings for all researchers).
* Provide more reports.  Currently the report only shows the average wait time for all jobs in the specified simulation.
* Various smaller improvements, in code comments
* Add some unit tests.
* Improve this README :)
