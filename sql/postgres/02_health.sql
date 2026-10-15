SELECT current_database(), current_user, current_setting('wal_level') AS wal_level;
SELECT pubname, schemaname, tablename FROM pg_publication_tables
WHERE pubname = 'payments_publication';
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
FROM pg_replication_slots WHERE slot_name = 'payments_cdc';
