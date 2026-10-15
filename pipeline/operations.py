from datetime import datetime, timedelta, timezone
from decimal import Decimal
import csv
import os
from pathlib import Path

from .cloud import request
from .config import outputs, require, save_evidence


def metrics():
    public, _ = outputs()
    require('CONFLUENT_CLOUD_API_KEY', 'CONFLUENT_CLOUD_API_SECRET')
    auth = {'key': os.environ['CONFLUENT_CLOUD_API_KEY'], 'secret': os.environ['CONFLUENT_CLOUD_API_SECRET']}
    end = datetime.now(timezone.utc) - timedelta(minutes=5)
    start = end - timedelta(minutes=15)
    iso = lambda d: d.isoformat().replace('+00:00', 'Z')
    results = {}
    for metric in ['received_bytes', 'sent_bytes']:
        results[metric] = request('https://api.telemetry.confluent.cloud', '/v2/metrics/cloud/query', auth, 'POST', {
            'aggregations': [{'metric': 'io.confluent.kafka.server/' + metric}],
            'filter': {'field': 'resource.kafka.id', 'op': 'EQ', 'value': public['cluster_id']},
            'granularity': 'PT1M', 'intervals': [f'{iso(start)}/{iso(end)}'],
        })
    save_evidence('metrics', results)
    if any(not response.get('data') for response in results.values()):
        raise RuntimeError('Ha metricas sem amostras. Aguarde ingestao/atraso da telemetria e repita.')


def costs(path: Path):
    """Soma linhas normalizadas da fatura, sem inventar tarifas de servicos."""
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ['service', 'quantity', 'unit', 'unit_price', 'currency']:
            raise ValueError('Use service,quantity,unit,unit_price,currency no CSV de custos.')
        rows = list(reader)
    if not rows:
        raise ValueError('CSV sem custos; ausencia de fatura nao e custo zero.')
    currencies = {r['currency'] for r in rows}
    if len(currencies) != 1:
        raise ValueError('Nao some moedas diferentes. Normalize antes.')
    total = Decimal(0)
    for row in rows:
        quantity, price = Decimal(row['quantity']), Decimal(row['unit_price'])
        if not quantity.is_finite() or not price.is_finite() or quantity < 0 or price < 0:
            raise ValueError('Quantidade e tarifa devem ser finitas e nao negativas.')
        row['subtotal'] = quantity * price
        total += row['subtotal']
    save_evidence('cost', {'source_file': path.name, 'currency': next(iter(currencies)),
                            'items': rows, 'total': total})
