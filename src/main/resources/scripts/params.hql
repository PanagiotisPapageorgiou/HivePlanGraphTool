ADD JAR ${nameNode}${MY_LIB};
ADD JAR ${nameNode}/user/hadoop/libs/json-serde-1.3.jar;
CREATE EXTERNAL TABLE table1 (
  name STRING
)
STORED AS RCFILE
LOCATION '${HDFS_PATH}/rc';