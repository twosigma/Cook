# Spark Support for Cook

Cook's integration with Spark isn't yet merged into the mainline of Spark. Cook
provides two patches for spark 1.5.0 and 1.6.1, the latter includes support for
dynamic allocation.

To apply the patch you need to clone spark repository:

```
git clone git@github.com:apache/spark.git
```

## Instructions for spark 1.5.0

```
cd spark
git checkout v1.5.0
git apply --stat 0001-Add-cook-support-for-spark-v1.5.0.patch
git apply --check 0001-Add-cook-support-for-spark-v1.5.0.patch
git am < 0001-Add-cook-support-for-spark-v1.5.0.patch
```

## Instructions for spark 1.6.1

```
cd spark
git checkout v1.6.1
git apply --stat 0001-Add-cook-support-for-spark-v1.6.1.patch
git apply --check 0001-Add-cook-support-for-spark-v1.6.1.patch
git am < 0001-Add-cook-support-for-spark-v1.6.1.patch
```

## Instructions

```
cd spark
git checkout v1.6.1
git apply --stat 0001-Add-cook-support-for-spark-v2.0.1.patch
git apply --check 0001-Add-cook-support-for-spark-v2.0.1.patch
git am < 0001-Add-cook-support-for-spark-v2.0.1.patch
```


## Installation

Please refer to [Building Spark](http://spark.apache.org/docs/latest/building-spark.html)
guide for more information about building and installing Spark.

## Running spark with Cook master

Once you've built Spark, you can run a Spark shell. To do this (assuming Cook is
running with the default testing configuration on `localhost`) you can start Spark
with the command line:

```
./bin/spark-shell --master cook://$USERNAME@localhost:12321 --conf spark.cores.max=1
```

You must specify the `spark.cores.max`, or else it won't launch any Spark executors.
You can set that property to a bigger number to use more resources and thus run faster.
To change the memory per executor, you can change the `spark.executor.memory`
property, e.g. `--conf spark.executor.memory=500m`.

### Spark 1.6.1 with dynamic allocation

If you're using spark 1.6.1 you can enable Dynamic Allocation (which additionally
requires shuffle-service):

```
./sbin/start-shuffle-service.sh

./bin/spark-shell --master cook://$USERNAME@localhost:12321 \
  --conf spark.cores.max=40 \
  --conf spark.cook.cores.per.job.max=4 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.initialExecutors=5 \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=10
```

For more configuration options please refer to [Dynamic Allocation](http://spark.apache.org/docs/latest/configuration.html#dynamic-allocation) documentation.

© Two Sigma Open Source, LLC
