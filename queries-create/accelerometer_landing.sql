CREATE EXTERNAL TABLE IF NOT EXISTS `mlp-stedi-db`.`accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "Initial's accelerometer's data"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://mlp-stedi-lakehouse/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data' = 'false');