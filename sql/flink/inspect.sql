-- Execute separadamente no workspace SQL selecionando payments-lab / payments.
SHOW CREATE TABLE `payments.public.transactions`;
SHOW CREATE TABLE `payments.public.accounts`;
SELECT event_time, CURRENT_WATERMARK(event_time), op
FROM `payments.public.transactions`;
SELECT * FROM `payments.transactions_enriched`;
SELECT * FROM `payments.fraud_alerts`;
