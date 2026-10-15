CREATE TABLE IF NOT EXISTS `payments.transactions_enriched` (
  transaction_id STRING NOT NULL,
  account_id STRING,
  card_token STRING,
  amount_minor BIGINT,
  currency STRING,
  occurred_at_ms BIGINT,
  status_at_creation STRING,
  account_name STRING,
  country STRING,
  risk_tier STRING,
  PRIMARY KEY (transaction_id) NOT ENFORCED
)
DISTRIBUTED BY HASH(transaction_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.cleanup-policy' = 'compact',
  'kafka.retention.time' = '7 d'
);
