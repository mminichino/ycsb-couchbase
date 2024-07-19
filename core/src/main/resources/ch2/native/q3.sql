WITH co as (SELECT o.o_id, o.o_w_id, o.o_d_id, o.o_entry_d, o.o_orderline
            FROM orders o,
                 customer c
            WHERE c.c_state LIKE 'A%'
              AND c.c_id = o.o_c_id
              AND c.c_w_id = o.o_w_id
              AND c.c_d_id = o.o_d_id
              AND o.o_entry_d /*+ skip-index */ < '2017-03-15 00:00:00.000000')
SELECT co.o_id, co.o_w_id, co.o_d_id, SUM(ol.ol_amount) as revenue, co.o_entry_d
FROM co,
     co.o_orderline ol,
     neworder no
WHERE no.no_w_id = co.o_w_id AND no.no_d_id = co.o_d_id AND no.no_o_id = co.o_id
GROUP BY co.o_id, co.o_w_id, co.o_d_id, co.o_entry_d
ORDER BY revenue DESC, co.o_entry_d
