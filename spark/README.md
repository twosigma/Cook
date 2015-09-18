# Spark Support for Cook

Cook's integration with Spark isn't yet merged into the mainline of Spark.
To add Cook support for Spark, go to a checkout of the Spark git repository, and do `git checkout v1.5.0`.
That's the commit that this patch should apply cleanly to.

First do `git apply --stat 0001-Add-cook-support.patch` to see the stats.
Then, do `git apply --check 0001-Add-cook-support.patch` to check for any errors before applying.
If that works, do `git am < 0001-Add-cook-support.patch` to apply the patch.

Once you've built Spark, you can run a Spark shell.
To do this (assuming Cook is running with the default testing configuration on `localhost`), you can start Spark with the command line `MASTER=cook://$username:passwordIgnored@localhost:12321 ./bin/spark-shell --conf spark.cores.max=1`

© Two Sigma Open Source, LLC
