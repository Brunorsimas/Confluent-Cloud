"""Oraculo finito para testes: nao executa CDC, Flink, watermarks ou rede."""
from collections import defaultdict


def expected_alerts(events: list[dict]) -> list[dict]:
    cards = defaultdict(list)
    for event in events:
        if event and event.get('op') in {'c', 'r'} and event.get('after'):
            row = event['after']
            cards[row['card_token']].append(row)
    result = []
    for card, rows in cards.items():
        rows.sort(key=lambda row: (row['occurred_at_ms'], row['transaction_id']))
        for index in range(2, len(rows)):
            window = rows[index - 2:index + 1]
            if len({r['transaction_id'] for r in window}) != 3:
                continue
            if window[-1]['occurred_at_ms'] - window[0]['occurred_at_ms'] <= 60000:
                result.append({
                    'alert_id': f'{card}:{window[-1]["transaction_id"]}',
                    'transaction_ids': [r['transaction_id'] for r in window],
                    'transaction_count': 3,
                })
    return result
