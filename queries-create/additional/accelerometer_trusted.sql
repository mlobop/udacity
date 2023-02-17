CREATE EXTERNAL TABLE IF NOT EXISTS `mlp-stedi-db`.`accelerometer_trusted` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "Accelerometer's data for the customers that have agreed to share"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
LOCATION 's3://mlp-stedi-lakehouse/accelerometer/trusted/'
TBLPROPERTIES ('has_encrypted_data' = 'false');