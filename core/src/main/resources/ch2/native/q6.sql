SELECT SUM(ol.ol_amount) as revenue
FROM orders o,
     o.o_orderline ol
WHERE ol.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND ol.ol_delivery_d < '2017-01-01 00:00:00.000000'
  AND ol.ol_amount > 600
