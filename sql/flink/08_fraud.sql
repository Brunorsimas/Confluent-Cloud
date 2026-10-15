INSERT INTO `payments.fraud_alerts`
WITH creations AS (
  SELECT
    `after`.transaction_id AS transaction_id,
    `after`.account_id AS account_id,
    `after`.card_token AS card_token,
    event_time
  FROM `payments.public.transactions`
  WHERE op IN ('c', 'r') AND `after` IS NOT NULL
), matches AS (
  SELECT * FROM creations
  MATCH_RECOGNIZE (
    PARTITION BY card_token
    ORDER BY event_time, transaction_id
    MEASURES
      A.transaction_id AS first_transaction_id,
      B.transaction_id AS second_transaction_id,
      C.transaction_id AS third_transaction_id,
      C.account_id AS account_id,
      A.event_time AS first_event_time,
      C.event_time AS last_event_time
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    -- O limite explicito em C inclui exatamente 60s. WITHIN limita o estado,
    -- com 1s de folga para nao expirar o padrao na borda inclusiva de 60s.
    PATTERN (A B C) WITHIN INTERVAL '61' SECOND
    DEFINE
      A AS TRUE,
      B AS B.transaction_id <> A.transaction_id,
      C AS C.transaction_id <> A.transaction_id
        AND C.transaction_id <> B.transaction_id
        AND C.event_time <= A.event_time + INTERVAL '60' SECOND
  )
)
SELECT
  CONCAT(m.card_token, ':', m.third_transaction_id),
  m.card_token,
  m.account_id,
  m.first_transaction_id,
  m.second_transaction_id,
  m.third_transaction_id,
  m.first_event_time,
  m.last_event_time,
  CAST(3 AS BIGINT),
  'three-per-card-60s-v1',
  a.account_name,
  a.country,
  a.risk_tier
FROM matches m
LEFT JOIN `payments.public.accounts` a ON m.account_id = a.account_id;
