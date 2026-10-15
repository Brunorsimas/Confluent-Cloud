from __future__ import annotations

import base64
import copy
import json
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .config import ROOT, outputs, save_evidence


class ApiError(RuntimeError):
    def __init__(self, status: int, path: str):
        self.status = status
        super().__init__(f'HTTP {status} em {path}; confira credenciais, permissoes e o console.')


def request(base: str, path: str, credentials: dict, method='GET', body=None):
    if not base.startswith('https://'):
        raise ValueError('API Cloud exige HTTPS')
    auth = base64.b64encode(f'{credentials["key"]}:{credentials["secret"]}'.encode()).decode()
    headers = {'Authorization': f'Basic {auth}', 'Content-Type': 'application/json'}
    data = None if body is None else json.dumps(body).encode()
    req = Request(base.rstrip('/') + path, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=45) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except HTTPError as error:
        # Respostas de APIs podem ecoar configuracoes sensiveis.
        raise ApiError(error.code, path) from None


def schema_variants(schema: dict) -> tuple[dict, dict]:
    compatible, incompatible = copy.deepcopy(schema), copy.deepcopy(schema)
    if not any(field['name'] == 'audit_tag' for field in compatible['fields']):
        compatible['fields'].append({'name': 'audit_tag', 'type': ['null', 'string'], 'default': None})
    operation = next((f for f in incompatible['fields'] if f['name'] == 'op'), None)
    if operation is None:
        raise ValueError('O subject nao tem envelope CDC com op')
    operation['type'] = 'long'
    operation.pop('default', None)
    return compatible, incompatible


def schema_test() -> None:
    public, secrets = outputs()
    subject = 'payments.public.transactions-value'
    encoded = quote(subject, safe='')
    auth = secrets['operator_registry']
    base = public['registry_url']
    original = request(base, f'/subjects/{encoded}/versions/latest', auth)
    schema = json.loads(original['schema'])
    request(base, f'/config/{encoded}', auth, 'PUT', {'compatibility': 'FULL_TRANSITIVE'})
    good, bad = schema_variants(schema)
    payload = lambda value: {'schemaType': 'AVRO', 'schema': json.dumps(value)}
    path = f'/compatibility/subjects/{encoded}/versions/latest?verbose=true'
    accepted = request(base, path, auth, 'POST', payload(good))
    rejected = request(base, path, auth, 'POST', payload(bad))
    if accepted.get('is_compatible') is not True or rejected.get('is_compatible') is not False:
        raise RuntimeError('Schema Registry respondeu diferente do esperado; evolucao interrompida.')
    registered = request(base, f'/subjects/{encoded}/versions', auth, 'POST', payload(good))
    try:
        request(base, f'/subjects/{encoded}/versions', auth, 'POST', payload(bad))
    except ApiError as error:
        if error.status != 409:
            raise
    else:
        raise RuntimeError('O Registry aceitou a quebra de contrato; revise a compatibilidade.')
    save_evidence('contract', {
        'subject': subject, 'compatibility': 'FULL_TRANSITIVE',
        'original_version': original['version'], 'compatible_schema_id': registered['id'],
        'optional_field_accepted': True, 'op_type_change_rejected_http': 409,
    })


class Flink:
    def __init__(self):
        self.public, secrets = outputs()
        self.credentials = secrets['flink']
        self.path = (f'/sql/v1/organizations/{self.public["organization_id"]}'
                     f'/environments/{self.public["environment_id"]}/statements')

    def call(self, suffix='', method='GET', body=None):
        return request(self.public['flink_url'], self.path + suffix, self.credentials, method, body)

    def wait(self, name: str, continuous: bool, timeout=300) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.call('/' + name)['status']
            phase = status['phase']
            if phase == 'COMPLETED' or (continuous and phase == 'RUNNING'):
                return {'name': name, 'phase': phase}
            if phase in {'FAILED', 'STOPPED', 'DELETING'}:
                raise RuntimeError(f'Flink {name}: {phase}. Consulte o erro no console.')
            time.sleep(3)
        raise TimeoutError(f'Flink {name}: limite de {timeout}s; job pode continuar no Cloud.')

    def deploy(self):
        results = []
        for path in sorted((ROOT / 'sql' / 'flink').glob('[0-9][0-9]_*.sql')):
            name = 'payments-' + path.stem.replace('_', '-')
            statement = path.read_text(encoding='utf-8')
            try:
                existing = self.call('/' + name)
                if existing['spec']['statement'] != statement:
                    raise RuntimeError(f'{name} mudou. Remova o statement no Cloud antes de reaplicar.')
            except ApiError as error:
                if error.status != 404:
                    raise
                self.call(method='POST', body={
                    'name': name,
                    'spec': {
                        'statement': statement, 'compute_pool_id': self.public['pool_id'],
                        'principal': self.public['operator_id'], 'stopped': False,
                        'properties': {
                            'sql.current-catalog': self.public['catalog'],
                            'sql.current-database': self.public['database'],
                            'sql.local-time-zone': 'UTC',
                            'sql.tables.scan.startup.mode': 'earliest-offset',
                        },
                    },
                })
            result = self.wait(name, continuous=path.name.startswith(('07_', '08_')))
            print(json.dumps(result))
            results.append(result)
        save_evidence('flink', results)

    def status(self):
        results = []
        for path in sorted((ROOT / 'sql' / 'flink').glob('[0-9][0-9]_*.sql')):
            name = 'payments-' + path.stem.replace('_', '-')
            try:
                obj = self.call('/' + name)
                results.append({'name': name, 'phase': obj['status']['phase']})
            except ApiError as error:
                if error.status != 404:
                    raise
                results.append({'name': name, 'phase': 'ABSENT'})
        save_evidence('flink-status', results)

    def delete(self):
        for path in sorted((ROOT / 'sql' / 'flink').glob('[0-9][0-9]_*.sql'), reverse=True):
            name = 'payments-' + path.stem.replace('_', '-')
            try:
                self.call('/' + name, 'DELETE')
                print(f'{name}: removido')
            except ApiError as error:
                if error.status != 404:
                    raise
