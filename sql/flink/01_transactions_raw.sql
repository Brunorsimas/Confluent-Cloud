-- Abre o envelope como um registro Avro comum; preserva op/before/after.
-- Sem isto a inferencia automatica entrega changelog, incompativel com CEP.
ALTER TABLE `payments.public.transactions` SET (
  'value.format' = 'avro-registry',
  'changelog.mode' = 'append'
);
