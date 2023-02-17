CREATE EXTERNAL TABLE IF NOT EXISTS `mlp-stedi-db`.`customer_trusted` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `shareWithFriendsAsOfDate` bigint
) COMMENT "Filtered non-null shareWithResearchAsOfDate values customer's data"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'TRUE'
)
LOCATION 's3://mlp-stedi-lakehouse/customer/landing/'
TBLPROPERTIES ('has_encrypted_data' = 'false');