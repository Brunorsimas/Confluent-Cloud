from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

from .config import ROOT, outputs, save_evidence


def client(identity='reader', group=None):
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    public, secrets = outputs()
    auth = secrets[f'{identity}_kafka']
    registry_auth = secrets[f'{identity}_registry']
    consumer = Consumer({
        'bootstrap.servers': public['bootstrap'].removeprefix('SASL_SSL://'),
        'security.protocol': 'SASL_SSL', 'sasl.mechanism': 'PLAIN',
        'sasl.username': auth['key'], 'sasl.password': auth['secret'],
        'group.id': group or f'payments-{identity}-{uuid.uuid4().hex[:12]}',
        'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
        'enable.auto.offset.store': False, 'isolation.level': 'read_committed',
    })
    registry = SchemaRegistryClient({
        'url': public['registry_url'],
        'basic.auth.user.info': f'{registry_auth["key"]}:{registry_auth["secret"]}',
    })
    return consumer, AvroDeserializer(registry), AvroDeserializer(registry)


def decode(message, key_decoder, value_decoder):
    from confluent_kafka.serialization import SerializationContext, MessageField
    topic = message.topic()
    return {
        'topic': topic, 'partition': message.partition(), 'offset': message.offset(),
        'key': key_decoder(message.key(), SerializationContext(topic, MessageField.KEY)),
        'value': value_decoder(message.value(), SerializationContext(topic, MessageField.VALUE)),
    }


def consume(topic: str, seconds: int, group: str):
    consumer, key_decoder, value_decoder = client(group=group)
    directory = ROOT / '.runtime'
    directory.mkdir(exist_ok=True)
    total = 0
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + seconds
        with (directory / 'consumed.jsonl').open('a', encoding='utf-8') as stream:
            while time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(f'Kafka: {message.error().name()}')
                event = decode(message, key_decoder, value_decoder)
                line = json.dumps(event, default=str, ensure_ascii=False)
                stream.write(line + '\n')
                stream.flush()
                os.fsync(stream.fileno())
                # Confirma somente depois da persistencia. Replay pode duplicar o log;
                # dedupe por (topic, partition, offset) ou materialize pela chave.
                consumer.commit(message=message, asynchronous=False)
                print(line)
                total += 1
    finally:
        consumer.close()
    if not total:
        raise RuntimeError('Nenhuma mensagem consumida no intervalo; isso nao comprova o pipeline.')
    save_evidence('consumer', {'topic': topic, 'group': group, 'messages': total})


def cdc_probe(seconds=120):
    from confluent_kafka import TopicPartition
    from .postgres import connect
    topic = 'payments.public.accounts'
    identity = 'probe-' + uuid.uuid4().hex
    consumer, key_decoder, value_decoder = client('operator')
    seen = {}
    try:
        metadata = consumer.list_topics(topic, timeout=20)
        partitions = metadata.topics[topic].partitions
        if not partitions:
            raise RuntimeError('Topico de contas sem particoes')
        starts = []
        for partition in partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=20)
            starts.append(TopicPartition(topic, partition, high))
        consumer.assign(starts)
        with connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT account_name, country, risk_tier FROM public.accounts ORDER BY account_id LIMIT 1')
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError('Carregue ao menos uma conta antes de cdc-probe.')
                cursor.execute('INSERT INTO public.accounts VALUES (%s, %s, %s, %s)', (identity, *row))
                connection.commit()
                risk = 'high' if row[2] != 'high' else 'low'
                cursor.execute('UPDATE public.accounts SET risk_tier = %s WHERE account_id = %s', (risk, identity))
                connection.commit()
                cursor.execute('DELETE FROM public.accounts WHERE account_id = %s', (identity,))
                connection.commit()
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and set(seen) != {'c', 'u', 'd', 'tombstone'}:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f'Kafka: {message.error().name()}')
            event = decode(message, key_decoder, value_decoder)
            if (event['key'] or {}).get('account_id') != identity:
                continue
            op = event['value']['op'] if event['value'] is not None else 'tombstone'
            seen[op] = event
        if set(seen) != {'c', 'u', 'd', 'tombstone'}:
            raise RuntimeError(f'CDC incompleto: recebido {sorted(seen)}; esperado c/u/d/tombstone.')
        if seen['u']['value']['before']['risk_tier'] == seen['u']['value']['after']['risk_tier']:
            raise RuntimeError('CDC update sem a mudanca esperada')
        save_evidence('cdc', {'probe_account_id': identity, 'events': list(seen.values())})
    finally:
        consumer.close()
        # Recupera a linha de diagnostico se houver falha entre os commits.
        with connect() as connection:
            connection.execute('DELETE FROM public.accounts WHERE account_id = %s', (identity,))


def verify(expected_path: Path, seconds=180):
    expected = json.loads(expected_path.read_text(encoding='utf-8'))
    wanted = {a['alert_id']: a for a in expected['expected_alerts']}
    if not wanted:
        raise ValueError('O cenario deve conter alertas esperados para comprovar a regra.')
    consumer, key_decoder, value_decoder = client()
    alerts, enriched = {}, {}
    try:
        consumer.subscribe(['payments.fraud_alerts', 'payments.transactions_enriched'])
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f'Kafka: {message.error().name()}')
            event = decode(message, key_decoder, value_decoder)
            target = alerts if event['topic'].endswith('fraud_alerts') else enriched
            field = 'alert_id' if target is alerts else 'transaction_id'
            key = event['key'][field]
            if event['value'] is None:
                target.pop(key, None)
            else:
                target[key] = event['value']
        if set(alerts) != set(wanted):
            raise RuntimeError(f'Alertas divergentes: esperado={sorted(wanted)}, recebido={sorted(alerts)}')
        for key, row in alerts.items():
            ids = [row[f'{position}_transaction_id'] for position in ('first', 'second', 'third')]
            if ids != wanted[key]['transaction_ids'] or row['transaction_count'] != 3 or not row['account_name']:
                raise RuntimeError(f'Alerta {key}: contagem, transacoes ou enriquecimento incorretos')
        if len(enriched) != expected['transactions'] or any(not r['account_name'] for r in enriched.values()):
            raise RuntimeError('Quantidade/enriquecimento de transacoes divergente')
        save_evidence('end-to-end', {'verified': True, 'alerts': alerts, 'enriched_count': len(enriched)})
    finally:
        consumer.close()


def acl_test(seconds=30):
    from confluent_kafka import KafkaError
    consumer, _, _ = client()
    try:
        consumer.subscribe(['payments.public.transactions'])
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error() and message.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                save_evidence('access', {'reader_cannot_read_source': True,
                                         'broker_error': 'TOPIC_AUTHORIZATION_FAILED'})
                return
            if message.error():
                raise RuntimeError(f'Erro diferente de autorizacao: {message.error().name()}')
            raise RuntimeError('Leitor conseguiu ler origem; revise ACLs.')
        raise RuntimeError('Timeout nao comprova negacao de acesso; teste inconclusivo.')
    finally:
        consumer.close()
