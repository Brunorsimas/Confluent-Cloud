ALTER TABLE `payments.public.transactions`
MODIFY WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND;
