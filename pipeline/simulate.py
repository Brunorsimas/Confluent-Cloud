"""Base pequena, deterministica e exclusivamente sintetica autorizada pelo usuario."""
import csv
from datetime import datetime, timezone
import json
from pathlib import Path

from .dataset import ACCOUNTS, TRANSACTIONS
from .reference import expected_alerts


def generate(directory: Path, base_ms: int):
    directory.mkdir(parents=True, exist_ok=True)
    files = [directory / name for name in ('accounts.csv', 'transactions.csv', 'expected.json')]
    if any(path.exists() for path in files):
        raise ValueError('Destino ja contem dados. Escolha outra pasta; nada foi sobrescrito.')
    accounts = [dict(zip(ACCOUNTS, [f'acc-{i}', f'Conta sintetica {i}', 'BR', 'low'])) for i in range(1, 4)]
    # Tempos relativos: cruzamento de minuto, 4 compras, borda 60s e fora da janela.
    cases = {
        'card-burst': [50000, 70000, 100000, 105000],
        'card-boundary': [10000, 40000, 70000],
        'card-outside': [10000, 40000, 70001],
        'card-advance': [180000],
    }
    transactions = []
    for number, (card, times) in enumerate(cases.items()):
        for index, offset in enumerate(times, 1):
            transactions.append(dict(zip(TRANSACTIONS, [
                f'{card}-{index}', f'acc-{number % 3 + 1}', card, 1000 + index * 100,
                'BRL', base_ms + offset, 'approved',
            ])))
    transactions.sort(key=lambda r: (r['occurred_at_ms'], r['transaction_id']))
    for path, columns, rows in [(files[0], ACCOUNTS, accounts), (files[1], TRANSACTIONS, transactions)]:
        with path.open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    alerts = expected_alerts([{'op': 'c', 'after': row} for row in transactions])
    expected = {'synthetic': True, 'base_time_utc': datetime.fromtimestamp(base_ms / 1000, timezone.utc).isoformat(),
                'accounts': len(accounts), 'transactions': len(transactions), 'expected_alerts': alerts}
    files[2].write_text(json.dumps(expected, indent=2) + '\n', encoding='utf-8')
    return expected
