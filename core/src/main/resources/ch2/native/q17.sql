SELECT SUM(ol.ol_amount) / 2.0 AS AVG_yearly
FROM (SELECT i.i_id, AVG(ol1.ol_quantity) AS a
      FROM item i,
           orders o1,
           o1.o_orderline ol1
      WHERE i.i_data LIKE '%b'
        AND ol1.ol_i_id = i.i_id
      GROUP BY i.i_id) t,
     orders o,
     o.o_orderline ol
WHERE ol.ol_i_id = t.i_id
  AND ol.ol_quantity < t.a
