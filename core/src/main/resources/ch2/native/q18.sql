SELECT c.c_last, c.c_id o_id, o.o_entry_d, o.o_ol_cnt, SUM(ol.ol_amount)
FROM orders o,
     o.o_orderline ol,
     customer c
WHERE c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
GROUP BY o.o_id, o.o_w_id, o.o_d_id, c.c_id, c.c_last, o.o_entry_d, o.o_ol_cnt
HAVING SUM(ol.ol_amount) > 200
ORDER BY SUM(ol.ol_amount) DESC, o.o_entry_d LIMIT 100
