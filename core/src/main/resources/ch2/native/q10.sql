SELECT c.c_id, c.c_last, SUM(ol.ol_amount) as revenue, c.c_city, c.c_phone, n.n_name
FROM nation n,
     customer c,
     orders o,
     o.o_orderline ol
WHERE c.c_id = o.o_c_id
  AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id
  AND o.o_entry_d >= '2015-10-01 00:00:00.000000'
  AND o.o_entry_d < '2016-01-01 00:00:00.000000'
  AND n.n_nationkey = string_to_codepoint(c.c_state)[0]
GROUP BY c.c_id, c.c_last, c.c_city, c.c_phone, n.n_name
ORDER BY revenue DESC LIMIT 20
