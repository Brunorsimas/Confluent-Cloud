INSERT INTO `payments.transactions_enriched`
SELECT
  t.`after`.transaction_id,
  t.`after`.account_id,
  t.`after`.card_token,
  t.`after`.amount_minor,
  t.`after`.currency,
  t.`after`.occurred_at_ms,
  t.`after`.status,
  a.account_name,
  a.country,
  a.risk_tier
FROM `payments.public.transactions` t
LEFT JOIN `payments.public.accounts` a
  ON t.`after`.account_id = a.account_id
WHERE t.op IN ('c', 'r') AND t.`after` IS NOT NULL;
