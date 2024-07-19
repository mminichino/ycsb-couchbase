SELECT o.o_ol_cnt, COUNT(*) as order_COUNT
FROM orders o
WHERE o.o_entry_d >= '2015-07-01 00:00:00.000000'
  AND o.o_entry_d < '2015-10-01 00:00:00.000000'
  AND EXISTS (SELECT VALUE 1 FROM o.o_orderline ol WHERE ol.ol_delivery_d >= date_add_str(o.o_entry_d, 1, 'week'))
GROUP BY o.o_ol_cnt
ORDER BY o.o_ol_cnt
