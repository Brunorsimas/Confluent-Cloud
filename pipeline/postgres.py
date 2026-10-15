from __future__ import annotations

import os
from pathlib import Path

from .config import ROOT, require, save_evidence
from .dataset import ACCOUNTS, TRANSACTIONS, validate_dataset


def connect():
    import psycopg
    require('PGHOST', 'PGDATABASE', 'PGUSER', 'PGPASSWORD')
    return psycopg.connect(connect_timeout=15)


def initialize():
    from psycopg import sql
    require('CDC_USER', 'CDC_PASSWORD')
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute('SHOW wal_level')
            if cursor.fetchone()[0] != 'logical':
                raise RuntimeError('Habilite wal_level=logical no servidor e reinicie antes de continuar.')
            cursor.execute((ROOT / 'sql/postgres/01_tables.sql').read_text(encoding='utf-8'))
            user = os.environ['CDC_USER']
            cursor.execute('SELECT 1 FROM pg_roles WHERE rolname = %s', (user,))
            exists = cursor.fetchone() is not None
            # SQL composavel impede que identificadores/senhas virem comandos.
            cursor.execute(sql.SQL('{} ROLE {} WITH LOGIN PASSWORD {}').format(
                sql.SQL('ALTER' if exists else 'CREATE'), sql.Identifier(user),
                sql.Literal(os.environ['CDC_PASSWORD'])))
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication'")
            if cursor.fetchone():
                cursor.execute(sql.SQL('GRANT rds_replication TO {}').format(sql.Identifier(user)))
            else:
                cursor.execute(sql.SQL('ALTER ROLE {} WITH REPLICATION').format(sql.Identifier(user)))
            cursor.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(
                sql.Identifier(os.environ['PGDATABASE']), sql.Identifier(user)))
            cursor.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(user)))
            cursor.execute(sql.SQL('GRANT SELECT ON public.accounts, public.transactions TO {}').format(
                sql.Identifier(user)))
    save_evidence('postgres-init', {'tables': ['accounts', 'transactions'],
                                   'replica_identity': 'FULL', 'publication': 'payments_publication'})


def load(directory: Path):
    from psycopg import sql
    accounts, transactions = validate_dataset(directory)
    with connect() as connection:
        with connection.cursor() as cursor:
            for table, columns, rows in [('accounts', ACCOUNTS, accounts),
                                          ('transactions', TRANSACTIONS, transactions)]:
                statement = sql.SQL('INSERT INTO public.{} ({}) VALUES ({})').format(
                    sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, columns)),
                    sql.SQL(',').join(sql.Placeholder() for _ in columns))
                cursor.executemany(statement, [tuple(row[c] for c in columns) for row in rows])
    save_evidence('load', {'accounts': len(accounts), 'transactions': len(transactions),
                           'input_directory': directory.name})


def health():
    results = []
    with connect() as connection:
        with connection.cursor() as cursor:
            statements = (ROOT / 'sql/postgres/02_health.sql').read_text(encoding='utf-8').split(';')
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
                    columns = [c.name for c in cursor.description]
                    results.append([dict(zip(columns, row)) for row in cursor.fetchall()])
    save_evidence('postgres-health', results)


def cleanup():
    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT active FROM pg_replication_slots WHERE slot_name = 'payments_cdc'")
            row = cursor.fetchone()
            if row and row[0]:
                raise RuntimeError('Slot ativo: remova o conector antes de executar pg-cleanup.')
            cursor.execute((ROOT / 'sql/postgres/99_cleanup.sql').read_text(encoding='utf-8'))
    save_evidence('postgres-cleanup', {'slot': 'removed_or_absent', 'publication': 'removed_or_absent'})
