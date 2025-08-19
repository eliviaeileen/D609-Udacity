CREATE EXTERNAL TABLE `accelerometer_landing` (
  user string,
  x double,
  y double,
  z double,
  timeStamp bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://elivia-landing-zone/accelerometer_landing/'
TBLPROPERTIES ('classification'='json');
