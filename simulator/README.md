# Cook Simulator

This project is a CLI interface to generated simulated workloads, to run them against a real instance of Cook, and to report on how the jobs in a given simulation were handled.

## Example usage

First, edit your intended settings file (e.g. config/settings.edn) as you see fit.
Since the program exits after each command, you'll need to be running against a
Datomic with persistent storage.

<pre><code>
~/projects/cook/simulator $ lein run -c config/settings.edn setup-database


~/projects/cook/simulator $ lein run -c config/settings.edn generate-job-schedule -f schedule.edn
Loading settings from  config/settings.edn
Connecting to simulation database...
Writing job schedule to schedule.edn ...


~/projects/cook/simulator $ lein run -c config/settings.edn import-job-schedule -f schedule.edn
Importing job schedule from schedule.edn ...
New job schedule created:  382630046467067
To run a simulation using this schedule, try arguments simulate -e  382630046467067


~/projects/cook/simulator $ lein run -c config/settings.edn list-job-schedules
Loading settings from  config/settings.edn
Connecting to simulation database...
Listing available job schedules...
Connecting to simulation database...
Listing available job schedules...

|              ID |                                    Label |  Duration | Num Users |                      Created |
|-----------------+------------------------------------------+-----------+-----------------+------------------------------|
| 382630046467067 |                           Simplistic sim |   10 secs |               1 | Thu Apr 21 09:38:10 EDT 2016 |
| 382630046467937 |                           A Sim of Fours | 1800 secs |               4 | Mon Apr 25 10:10:44 EDT 2016 |
| 382630046475571 |                      Guaranteed pre-empt |   120 secs |               1 | Mon Apr 25 15:38:42 EDT 2016 |


~/projects/cook/simulator $ lein run -c config/settings.edn simulate -e 382630046467067 -l "Try with 4 mesos slaves"
Loading settings from  config/settings.edn
Connecting to simulation database...
Running a simulation for schedule  382630046467067 ...
Created simulation  387028093003449 .
Label is " Try with 4 mesos slaves ".
After jobs are finished, try running report -e  387028093003449 .
scheduling cook job with payload:  {"jobs":[{"name":"gdwocmuiel","priority":81,"max_retries":3,"max_runtime":86400000,"mem":464,"cpus":2.0,"uuid":"572b971c-9f35-43ac-800f-17f66e80993a","command":"sleep 5; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"name":"yaiqlzwhfm","priority":33,"max_retries":3,"max_runtime":86400000,"mem":623,"cpus":1.0,"uuid":"572b971e-6157-4f93-8c04-8a9b29f9ec33","command":"sleep 26; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"name":"rhlsepmuav","priority":39,"max_retries":3,"max_runtime":86400000,"mem":27,"cpus":2.0,"uuid":"572b971f-e2d9-4903-9e6f-1fb10f04b6a3","command":"sleep 19; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"name":"bdzwqfohcp","priority":77,"max_retries":3,"max_runtime":86400000,"mem":1007,"cpus":4.0,"uuid":"572b9720-f829-4d9b-be5a-d2c35af23e1b","command":"sleep 16; exit 0"}]}
"Elapsed time: 9486.311984 msecs"


~/projects/cook/simulator $ lein run -c config/settings.edn simulate -e 382630046467067 -l "Try with 8 mesos slaves"
Loading settings from  config/settings.edn
Connecting to simulation database...
Running a simulation for schedule  382630046467067 ...
Created simulation  387028093003501 .
Label is " Try with 8 mesos slaves ".
After jobs are finished, try running report -e  387028093003501 .
scheduling cook job with payload:  {"jobs":[{"name":"gdwocmuiel","priority":81,"max_retries":3,"max_runtime":86400000,"mem":464,"cpus":2.0,"uuid":"572b9aca-28e9-422b-9176-77d4bc8e9493","command":"sleep 5; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"name":"yaiqlzwhfm","priority":33,"max_retries":3,"max_runtime":86400000,"mem":623,"cpus":1.0,"uuid":"572b9acd-f0fc-41a9-bfa2-d2488093aea5","command":"sleep 26; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"name":"rhlsepmuav","priority":39,"max_retries":3,"max_runtime":86400000,"mem":27,"cpus":2.0,"uuid":"572b9ace-a50b-497d-816a-3ac0ff8b7c63","command":"sleep 19; exit 0"}]}
scheduling cook job with payload:  {"jobs":[{"name":"bdzwqfohcp","priority":77,"max_retries":3,"max_runtime":86400000,"mem":1007,"cpus":4.0,"uuid":"572b9acf-9431-4697-96b5-a59234ae7b5b","command":"sleep 16; exit 0"}]}
"Elapsed time: 9491.947411 msecs"


~/projects/cook/simulator $ lein run -c config/settings.edn list-sims -e 382630046467067
Loading settings from  config/settings.edn
Connecting to simulation database...
listing simulation runs for test 382630046467067 ...

|              ID |                   Label |                         When |
|-----------------+-------------------------+------------------------------|
| 387028092978391 |            With fenzo 5 | Fri Apr 22 09:12:26 EDT 2016 |
| 387028092978443 |            With fenzo 5 | Fri Apr 22 09:33:22 EDT 2016 |
| 387028093003501 | Try with 8 mesos slaves | Thu May 05 15:11:02 EDT 2016 |
| 387028092978235 |            With fenzo 2 | Thu Apr 21 09:40:53 EDT 2016 |
| 387028092978339 |            With fenzo 4 | Fri Apr 22 09:09:56 EDT 2016 |
| 387028092978287 |            With fenzo 3 | Fri Apr 22 09:06:49 EDT 2016 |
| 387028093003449 | Try with 4 mesos slaves | Thu May 05 14:55:19 EDT 2016 |
| 387028092978183 |              With fenzo | Thu Apr 21 09:39:02 EDT 2016 |


~/projects/cook/simulator $ lein run -c config/settings.edn report -e 387028093003449
Loading settings from  config/settings.edn
Connecting to simulation database...
Connecting to Cook database...
Analyzing sim  387028093003449 ...
4 jobs in sim...
Average wait time for jobs is 35.45025 seconds.
Average turnaround for jobs is 52.3165 seconds.
Average overhead for jobs is 35.8165 seconds.


~/projects/cook/simulator $ lein run -c config/settings.edn report -e 387028093003501
Loading settings from  config/settings.edn
Connecting to simulation database...
Connecting to Cook database...
Analyzing sim  387028093003501 ...
4 jobs in sim...
Average wait time for jobs is 25.48125 seconds.
Average turnaround for jobs is 42.20625 seconds.
Average overhead for jobs is 25.70625 seconds.


~/projects/cook/simulator $ lein run -c config/settings.edn compare -e 387028093003449 --compare 387028093003501 --metric overhead -f comparison.png
Loading settings from  config/settings.edn
Connecting to simulation database...
Connecting to Cook database...
Comparing overhead for sims (387028093003501) to baseline 387028093003449 ...
Outputting comparison image to comparison.png
</code></pre>

## Developing

If you'd like to extend the Simulator or just run a lot of sims quickly via
the REPL, [this document](doc/development.md) will help you get started.
