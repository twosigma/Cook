Optimizer
=========

The optimizer is intended to provide a longer term, holistic plan for the cluster that other components in Cook can consume to inform their operation.
Cook will provide a no-op implementation of an optimizer and allow for plugging in different implementations.

The optimizer is provided with the current queue, the jobs that are running, the offers that are available and a pluggable feed of hosts that can be purchased.
There are plans to support more plug-ins such as expected demand in the future.
With these  inputs, the optimizer produces a 'schedule' of suggestions of what hosts to purchase and matches of jobs and hosts at different time horizons.

There are plans to have the schedule be fed to the matcher so that it may treat the suggestions of the optimizer as soft constraints.

The specification of pluggable pieces can be found in [optimizer.clj](scheduler/src/cook/mesos/optimizer.clj).
