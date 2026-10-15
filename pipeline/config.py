from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def load_env(path: Path | None = None) -> None:
    """Le KEY=VALUE literalmente, sem executar comandos ou expandir variaveis."""
    path = path or ROOT / '.env'
    if not path.exists():
        return
    for number, line in enumerate(path.read_text(encoding='utf-8-sig').splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, sep, value = line.partition('=')
        if not sep or not key.strip().replace('_', '').isalnum():
            raise ValueError(f'.env: linha {number} invalida')
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


def require(*names: str) -> None:
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise ValueError('Configure no .env: ' + ', '.join(missing))


def tf_command(*args: str) -> list[str]:
    executable = os.environ.get('TERRAFORM', 'terraform')
    return [executable, f'-chdir={ROOT / "infra"}', *args]


def tf_env() -> dict[str, str]:
    env = dict(os.environ)
    for source, target in {
        'PGHOST': 'pg_host', 'PGPORT': 'pg_port', 'PGDATABASE': 'pg_database',
        'CDC_USER': 'cdc_user', 'CDC_PASSWORD': 'cdc_password',
    }.items():
        if env.get(source):
            env[f'TF_VAR_{target}'] = env[source]
    return env


def outputs() -> tuple[dict, dict]:
    result = subprocess.run(tf_command('output', '-json'), env=tf_env(),
                            capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError('Terraform output falhou; execute a fundacao primeiro.')
    data = json.loads(result.stdout)
    if not {'public', 'credentials'} <= data.keys():
        raise RuntimeError('State sem outputs do pipeline; execute terraform apply.')
    return data['public']['value'], data['credentials']['value']


def save_evidence(name: str, data: dict | list) -> None:
    # Somente dados explicitamente selecionados pelos comandos; nunca outputs brutos.
    from datetime import datetime, timezone
    directory = ROOT / 'evidence' / 'private'
    directory.mkdir(parents=True, exist_ok=True)
    document = {'recorded_at_utc': datetime.now(timezone.utc).isoformat(), 'result': data}
    (directory / f'{name}.json').write_text(
        json.dumps(document, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps(document, ensure_ascii=False, indent=2, default=str))
