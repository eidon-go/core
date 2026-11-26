-- Lets the tests read the isolation level of the current transaction
-- (performance_schema.events_transactions_current).
GRANT SELECT ON performance_schema.* TO 'app'@'%';
