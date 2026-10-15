from __future__ import annotations

import csv
from pathlib import Path

ACCOUNTS = ['account_id', 'account_name', 'country', 'risk_tier']
TRANSACTIONS = ['transaction_id', 'account_id', 'card_token', 'amount_minor',
                'currency', 'occurred_at_ms', 'status']


def read_csv(path: Path, columns: list[str]) -> list[dict]:
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != columns:
            raise ValueError(f'{path.name}: cabecalho esperado: {",".join(columns)}')
        rows = list(reader)
    if not rows:
        raise ValueError(f'{path.name}: arquivo sem registros')
    seen = set()
    for line, row in enumerate(rows, 2):
        if None in row or any(not isinstance(v, str) or not v.strip() for v in row.values()):
            raise ValueError(f'{path.name}:{line}: campo vazio ou coluna extra')
        identity = row[columns[0]]
        if identity in seen:
            raise ValueError(f'{path.name}:{line}: ID duplicado')
        seen.add(identity)
        if columns == ACCOUNTS:
            if len(row['country']) != 2 or row['risk_tier'] not in {'low', 'medium', 'high'}:
                raise ValueError(f'{path.name}:{line}: country/risk_tier invalido')
        else:
            for field in ('amount_minor', 'occurred_at_ms'):
                try:
                    row[field] = int(row[field])
                except ValueError:
                    raise ValueError(f'{path.name}:{line}: {field} deve ser inteiro') from None
                if not 0 < row[field] <= 9223372036854775807:
                    raise ValueError(f'{path.name}:{line}: {field} fora do intervalo BIGINT positivo')
            if len(row['currency']) != 3 or row['status'] not in {'pending', 'approved', 'declined', 'refunded'}:
                raise ValueError(f'{path.name}:{line}: currency/status invalido')
    return rows


def validate_dataset(directory: Path) -> tuple[list[dict], list[dict]]:
    accounts = read_csv(directory / 'accounts.csv', ACCOUNTS)
    transactions = read_csv(directory / 'transactions.csv', TRANSACTIONS)
    known = {row['account_id'] for row in accounts}
    if any(row['account_id'] not in known for row in transactions):
        raise ValueError('Transacao referencia conta ausente em accounts.csv')
    transactions.sort(key=lambda row: (row['occurred_at_ms'], row['transaction_id']))
    return accounts, transactions
