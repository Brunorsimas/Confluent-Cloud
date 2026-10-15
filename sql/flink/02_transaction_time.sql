ALTER TABLE `payments.public.transactions`
ADD (event_time AS TO_TIMESTAMP_LTZ(`after`.occurred_at_ms, 3));
