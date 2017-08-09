# 0.7.4
 - Fix Scala 2.10 Spark 2.0.X, 2.1.X context reuse reflection issue.
# 0.7.3
 - Re-add Scala 2.10 support up to and including Spark 2.2.X series
 - Attempt to make it so that users doing SQL tests without Hive don't need the hive jars.
 - Don't reset the SparkSession provider when in reuse mode.
 - Add workaround for inaccessiable active context info in Spark 2.0
 - Upgrade to Hadoop 2.8.1 for mini cluster
 - Change build env after travis changes
# 0.7.2
 - Add expiremental support to for reusing a SparkContext/Session accross multiple suites. For Spark 2.0+ only.
# 0.7.1
 - Upgrade mini cluster hadoop dependencies
 - Add support for Spark 2.2.0
 - YARNCluster now requires SPARK_HOME to be set so as to configure spark.yarn.jars (workaround for YARN bug from deprecated code in Spark 2.2).
# 0.7
 - Add Python RDD comparisions
 - Switch to JDK8 for Spark 2.1.1+
 - Add back Kafka tests
 - Make it easier to disable Hive support when running tests
 - Add Spark 2.1.1 to the build
 - Misc internal style cleanup (more help always welcome!)
 - README update
 - Some methods made protected which used to be public, recompile required.
# 0.6
 - Updated scalatest dependency to 3.0.1 (from 2.X) minor breaking changes with RNG
 - Updated scalacheck to 1.13.4
 - Added support for specifying custom generators for fields nested in `StructTypes`
   - Renamed `ColumnGenerator` to `Column`
   - Changed `ColumnGenerator` to the base class of column generators
   - Added `ColumnList` to specify custom generators for a list of columns
