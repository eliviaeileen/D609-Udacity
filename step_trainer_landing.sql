CREATE EXTERNAL TABLE `step_trainer_landing` (
  sensorReadingTime bigint,
  serialNumber string,
  distanceFromObject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://elivia-landing-zone/step_trainer_landing/'
TBLPROPERTIES ('classification'='json');
