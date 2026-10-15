CREATE TABLE IF NOT EXISTS `payments.fraud_alerts` (
  alert_id STRING NOT NULL,
  card_token STRING,
  account_id STRING,
  first_transaction_id STRING,
  second_transaction_id STRING,
  third_transaction_id STRING,
  first_event_time TIMESTAMP_LTZ(3),
  last_event_time TIMESTAMP_LTZ(3),
  transaction_count BIGINT,
  rule_id STRING,
  account_name STRING,
  country STRING,
  risk_tier STRING,
  PRIMARY KEY (alert_id) NOT ENFORCED
)
DISTRIBUTED BY HASH(alert_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.cleanup-policy' = 'compact',
  'kafka.retention.time' = '7 d'
);
