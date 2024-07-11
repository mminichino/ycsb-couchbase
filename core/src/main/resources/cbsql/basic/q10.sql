SELECT c.c_id
FROM customer c
WHERE c.c_id >= 10
GROUP BY c.c_id
ORDER BY c.c_id DESC LIMIT 20
