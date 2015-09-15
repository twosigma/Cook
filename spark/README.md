# Spark Support for Cook

Cook's integration with Spark isn't yet merged into the mainline of Spark.
To add Cook support for Spark, go to a checkout of the Spark git repository, and do `git checkout 4aaf48d46`.
That's the commit that this patch should apply cleanly to.

First do `git apply --stat 0001-Add-cook-scheduler-as-spark-scheduler-option.patch` to see the stats.
Then, do `git apply --check 0001-Add-cook-scheduler-as-spark-scheduler-option.patch` to check for any errors before applying.
If that works, do `git am < 0001-Add-cook-scheduler-as-spark-scheduler-option.patch` to apply the patch.

© Two Sigma Open Source, LLC
