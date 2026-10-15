ALTER TABLE `payments.public.accounts` SET (
  'value.format' = 'avro-debezium-registry',
  'changelog.mode' = 'retract'
);
