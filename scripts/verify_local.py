"""Registra saidas reais das verificacoes locais, sem credenciais de nuvem."""
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
terraform = os.environ.get('TERRAFORM', 'terraform')
checks = [
    ('python -m unittest discover -s tests -v', [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v']),
    ('python -m pip check', [sys.executable, '-m', 'pip', 'check']),
    ('terraform -chdir=infra fmt -check', [terraform, '-chdir=infra', 'fmt', '-check']),
    ('terraform -chdir=infra validate -no-color', [terraform, '-chdir=infra', 'validate', '-no-color']),
]
lines = ['LOCAL VALIDATION — NOT CLOUD EXECUTION', datetime.now(timezone.utc).isoformat()]
failed = False
for label, command in checks:
    result = subprocess.run(command, cwd=ROOT, text=True, encoding='utf-8', errors='replace', capture_output=True)
    lines.extend(['', f'$ {label}', result.stdout.strip(), result.stderr.strip(), f'exit_code={result.returncode}'])
    failed |= result.returncode != 0
report = '\n'.join(lines) + '\n'
directory = ROOT / 'evidence'
directory.mkdir(exist_ok=True)
(directory / 'local-validation.txt').write_text(report, encoding='utf-8')
print(report)
sys.exit(1 if failed else 0)
