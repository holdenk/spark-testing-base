#0.6
 - Updated scalatest dependency to 3.0.1 (from 2.X) minor breaking changes with RNG
 - Updated scalacheck to 1.13.4
 - Added support for specifying custom generators for fields nested in `StructTypes`
   - Renamed `ColumnGenerator` to `Column`
   - Changed `ColumnGenerator` to the base class of column generators
   - Added `ColumnList` to specify custom generators for a list of columns
