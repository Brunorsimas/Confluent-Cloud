Gere a base sintética com `python -m pipeline simulate` ou coloque aqui os CSVs
fornecidos pelo curso. A simulação foi autorizada pelo solicitante e gera 3 contas,
11 transações e um arquivo `expected.json` com 3 alertas esperados.
Arquivos nesta pasta são ignorados pelo Git; o gerador é versionado.

`accounts.csv`, UTF-8, cabeçalho exato:

```csv
account_id,account_name,country,risk_tier
```

`transactions.csv`, UTF-8, cabeçalho exato:

```csv
transaction_id,account_id,card_token,amount_minor,currency,occurred_at_ms,status
```

Mapeie os nomes de coluna do dataset para esse contrato antes da carga.
`amount_minor` é inteiro na menor unidade da moeda (centavos para BRL),
`occurred_at_ms` é Unix epoch em milissegundos UTC e `card_token` é um identificador
sintético/tokenizado, nunca o número real de cartão. Status aceitos: pending,
approved, declined, refunded. Risco: low, medium, high.

A carga é transacional, valida contas, referências, tipos e IDs duplicados antes
de escrever e falha se um ID já existir no banco. Não altera dados existentes.
O importador ordena transações por tempo para permitir replays históricos dentro
da tolerância do watermark. Contas devem ser carregadas antes das transações.
