from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from .config import ROOT, load_env, outputs, require, save_evidence, tf_command, tf_env


def parser():
    root = argparse.ArgumentParser(description='Pipeline Postgres -> Confluent Cloud -> Flink -> alertas')
    commands = root.add_subparsers(dest='command', required=True)
    for name in ['doctor', 'foundation', 'pg-init', 'pg-health', 'pg-cleanup', 'schema-test',
                 'flink-deploy', 'flink-status', 'flink-delete', 'cdc-probe', 'acl-test', 'metrics', 'connector-status']:
        commands.add_parser(name)
    terraform = commands.add_parser('tf')
    terraform.add_argument('arguments', nargs=argparse.REMAINDER)
    simulate = commands.add_parser('simulate')
    simulate.add_argument('--directory', type=Path, default=ROOT / 'data')
    simulate.add_argument('--base-time', default='2026-01-01T00:00:00+00:00')
    for name in ['validate-data', 'load']:
        command = commands.add_parser(name)
        command.add_argument('--directory', type=Path, default=ROOT / 'data')
    consume = commands.add_parser('consume')
    consume.add_argument('--topic', choices=['payments.fraud_alerts', 'payments.transactions_enriched'], default='payments.fraud_alerts')
    consume.add_argument('--seconds', type=int, default=90)
    consume.add_argument('--group', default='payments-reader-app')
    verify = commands.add_parser('verify')
    verify.add_argument('--expected', type=Path, default=ROOT / 'data/expected.json')
    verify.add_argument('--seconds', type=int, default=180)
    costs = commands.add_parser('costs')
    costs.add_argument('file', type=Path)
    return root


def doctor():
    packages = {}
    for name in ['confluent-kafka', 'psycopg', 'avro']:
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = 'missing'
    config = ['CONFLUENT_CLOUD_API_KEY', 'CONFLUENT_CLOUD_API_SECRET',
              'PGHOST', 'PGDATABASE', 'PGUSER', 'PGPASSWORD', 'CDC_USER', 'CDC_PASSWORD']
    report = {
        'python': sys.version.split()[0], 'packages': packages,
        'terraform_available': shutil.which(os.environ.get('TERRAFORM', 'terraform')) is not None,
        'docker_available': shutil.which('docker') is not None,
        'configuration_present': {name: bool(os.environ.get(name)) for name in config},
        'cloud_connection_checked': False,
    }
    save_evidence('doctor', report)


def main():
    load_env()
    args = parser().parse_args()
    command = args.command
    if command == 'doctor':
        doctor()
    elif command == 'tf':
        if not args.arguments:
            raise ValueError('Informe o subcomando Terraform, por exemplo: tf init')
        result = subprocess.run(tf_command(*args.arguments), env=tf_env(), check=False)
        return result.returncode
    elif command == 'foundation':
        public, _ = outputs()
        save_evidence('foundation', public)
    elif command == 'simulate':
        from .simulate import generate
        base = datetime.fromisoformat(args.base_time)
        if base.tzinfo is None:
            raise ValueError('base-time exige timezone, por exemplo +00:00')
        save_evidence('simulation', generate(args.directory, int(base.timestamp() * 1000)))
    elif command == 'validate-data':
        from .dataset import validate_dataset
        accounts, transactions = validate_dataset(args.directory)
        save_evidence('dataset', {'accounts': len(accounts), 'transactions': len(transactions), 'valid': True})
    elif command in {'pg-init', 'pg-health', 'pg-cleanup', 'load'}:
        from . import postgres
        if command == 'load':
            postgres.load(args.directory)
        else:
            {'pg-init': postgres.initialize, 'pg-health': postgres.health, 'pg-cleanup': postgres.cleanup}[command]()
    elif command.startswith('flink-') or command == 'schema-test':
        from .cloud import Flink, schema_test
        if command == 'schema-test':
            schema_test()
        else:
            flink = Flink()
            {'flink-deploy': flink.deploy, 'flink-status': flink.status, 'flink-delete': flink.delete}[command]()
    elif command in {'consume', 'verify', 'cdc-probe', 'acl-test'}:
        from . import kafka
        if command == 'consume':
            if not args.group.startswith('payments-reader-'):
                raise ValueError('O grupo deve iniciar com payments-reader- para corresponder a ACL.')
            kafka.consume(args.topic, args.seconds, args.group)
        elif command == 'verify':
            kafka.verify(args.expected, args.seconds)
        elif command == 'cdc-probe':
            kafka.cdc_probe()
        else:
            kafka.acl_test()
    elif command == 'connector-status':
        from .cloud import request
        require('CONFLUENT_CLOUD_API_KEY', 'CONFLUENT_CLOUD_API_SECRET')
        public, _ = outputs()
        connector = public['connector_id']
        if not connector:
            raise RuntimeError('Conector ainda nao foi habilitado.')
        connector = connector.split('/')[-1]
        path = f'/connect/v1/environments/{public["environment_id"]}/clusters/{public["cluster_id"]}/connectors/{connector}/status'
        auth = {'key': os.environ['CONFLUENT_CLOUD_API_KEY'], 'secret': os.environ['CONFLUENT_CLOUD_API_SECRET']}
        status = request('https://api.confluent.cloud', path, auth)
        report = {'connector': status.get('connector', {}).get('state'),
                  'tasks': [{'id': t.get('id'), 'state': t.get('state')} for t in status.get('tasks', [])]}
        save_evidence('connector', report)
        if report['connector'] != 'RUNNING' or not report['tasks'] or any(t['state'] != 'RUNNING' for t in report['tasks']):
            raise RuntimeError('Conector/tarefas ainda nao estao RUNNING.')
    elif command == 'metrics':
        from .operations import metrics
        metrics()
    elif command == 'costs':
        from .operations import costs
        costs(args.file)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except (ValueError, RuntimeError, FileNotFoundError, TimeoutError) as error:
        print(f'ERRO: {error}', file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        # Nunca imprimir DSN, payload de configuracao ou traceback com segredos.
        print(f'ERRO: {type(error).__name__}. Confira conectividade, permissoes e o console do servico.', file=sys.stderr)
        sys.exit(1)
