#0.7
 - Add Python RDD comparisions
 - Switch to JDK8 for Spark 2.1.1+
 - Add back Kafka tests
 - Make it easier to disable Hive support when running tests
 - Add Spark 2.1.1 to the build
 - Misc internal style cleanup (more help always welcome!)
 - README update
 - Some methods made protected which used to be public, recompile required.
#0.6
 - Updated scalatest dependency to 3.0.1 (from 2.X) minor breaking changes with RNG
 - Updated scalacheck to 1.13.4
 - Added support for specifying custom generators for fields nested in `StructTypes`
   - Renamed `ColumnGenerator` to `Column`
   - Changed `ColumnGenerator` to the base class of column generators
   - Added `ColumnList` to specify custom generators for a list of columns
