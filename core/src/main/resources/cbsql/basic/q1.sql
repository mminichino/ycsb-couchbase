SELECT *
FROM order_line ol
WHERE ol.ol_o_id > 20 OR ol.ol_o_id < 400
    LIMIT 10
