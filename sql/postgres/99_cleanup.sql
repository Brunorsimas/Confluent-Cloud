-- Execute SOMENTE depois de excluir o conector. Preserva tabelas e dados.
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots WHERE slot_name = 'payments_cdc' AND NOT active;
DROP PUBLICATION IF EXISTS payments_publication;
