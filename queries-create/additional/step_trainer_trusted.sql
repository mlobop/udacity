CREATE EXTERNAL TABLE IF NOT EXISTS `mlp-stedi-db`.`step_trainer_trusted` (
  `sensorReadingTime` string,
  `serialNumber` bigint,
  `distanceFromObject` int
) COMMENT "Step_trainer data of customer that have agreed to share"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'TRUE'
)
LOCATION 's3://mlp-stedi-lakehouse/step_trainer/trusted/'
TBLPROPERTIES ('has_encrypted_data' = 'false');