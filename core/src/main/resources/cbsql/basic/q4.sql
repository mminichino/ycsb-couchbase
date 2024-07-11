SELECT COUNT(*) AS order_COUNT
FROM orders o
WHERE o.o_id >= 10
LIMIT 10
