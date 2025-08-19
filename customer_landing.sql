CREATE EXTERNAL TABLE `customer_landing` (
  customername string,
  email string,
  phone string,
  birthday string,
  serialnumber string,
  registrationdate bigint,
  lastupdatedate bigint,
  sharewithresearchasofdate bigint,
  sharewithpublicasofdate bigint,
  sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://elivia-landing-zone/customer_landing/'
TBLPROPERTIES ('classification'='json');

