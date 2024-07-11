SELECT ol.ol_amount
FROM order_line ol
WHERE ol.ol_amount > 600
LIMIT 20
