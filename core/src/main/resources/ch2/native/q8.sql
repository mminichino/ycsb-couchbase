SELECT DATE_PART_STR(rn1coolis.o_entry_d, 'year') as l_year,
       ROUND((SUM(case when sun2.n_name = 'Germany' then rn1coolis.ol_amount else 0 end) / SUM(rn1coolis.ol_amount)),
             2)                                   as mkt_share
FROM (SELECT rn1cooli.o_entry_d, rn1cooli.ol_amount, s.s_w_id, s.s_i_id
      FROM stock s
               JOIN (SELECT o.o_entry_d, ol.ol_i_id, ol.ol_amount, ol.ol_supply_w_id
                     FROM orders o,
                          o.o_orderline ol,
                          item i
                              JOIN (SELECT c.c_id, c.c_w_id, c.c_d_id
                                    FROM customer c
                                             JOIN (SELECT n1.n_nationkey
                                                   FROM nation n1,
                                                        region r
                                                   WHERE n1.n_regionkey = r.r_regionkey
                                                     AND r.r_name = 'Europe') nr
                                                  ON nr.n_nationkey = string_to_codepoint(c.c_state)[0]) cnr
                                   ON cnr.c_id = o.o_c_id AND cnr.c_w_id = o.o_w_id AND cnr.c_d_id = o.o_d_id AND
                                      i.i_data LIKE '%b' AND i.i_id = ol.ol_i_id AND ol.ol_i_id < 1000 AND
                                      o.o_entry_d /*+ skip-index */ BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000') rn1cooli
                    ON rn1cooli.ol_i_id = s.s_i_id AND rn1cooli.ol_supply_w_id = s.s_w_id) rn1coolis
         JOIN (SELECT su.su_suppkey, n2.n_name
               FROM supplier su,
                    nation n2
               WHERE su.su_nationkey = n2.n_nationkey) sun2
              ON rn1coolis.s_w_id * rn1coolis.s_i_id MOD 10000 = sun2.su_suppkey
GROUP BY DATE_PART_STR(rn1coolis.o_entry_d, 'year')
ORDER BY l_year
