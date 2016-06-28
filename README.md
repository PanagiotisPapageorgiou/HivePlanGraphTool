# HivePlanToGraphTool
Simple tool that converts a Hive Plan to a total Graph of Operators.

Based on bobfreitas' hiveunit-mr2 library. Hiveunit-mr2 is a convenient wrapper around the existing MiniHiveServer2 and MiniCluster testing tools that are currently being used by the developers of Hive, MR2 & YARN.

This tool uses hiveunit-mr2 to extract the Query Plan from a HiveQL Query and turn it into a Graph of Operators.
