CREATE TABLE IF NOT EXISTS public.accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL,
    country TEXT NOT NULL CHECK (length(country) = 2),
    risk_tier TEXT NOT NULL CHECK (risk_tier IN ('low', 'medium', 'high'))
);

CREATE TABLE IF NOT EXISTS public.transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES public.accounts(account_id),
    card_token TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency TEXT NOT NULL CHECK (length(currency) = 3),
    occurred_at_ms BIGINT NOT NULL CHECK (occurred_at_ms > 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'declined', 'refunded'))
);

-- Necessario para before completo em updates/deletes e o retract do Flink.
ALTER TABLE public.accounts REPLICA IDENTITY FULL;
ALTER TABLE public.transactions REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'payments_publication') THEN
        CREATE PUBLICATION payments_publication FOR TABLE public.accounts, public.transactions;
    END IF;
END $$;
