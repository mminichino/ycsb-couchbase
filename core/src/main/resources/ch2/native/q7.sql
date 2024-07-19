SELECT su.su_nationkey                            as supp_nation,
       SUBSTR1(n1n2cools.c_state, 1, 1)           as cust_nation,
       DATE_PART_STR(n1n2cools.o_entry_d, 'year') as l_year,
       ROUND(SUM(n1n2cools.ol_amount), 2)         as revenue
FROM (select n1n2cool.c_state, n1n2cool.o_entry_d, n1n2cool.ol_amount, n1n2cool.n1key, s.s_w_id, s.s_i_id
      FROM stock s
               JOIN (SELECT o.o_entry_d, ol.ol_supply_w_id, ol.ol_i_id, n1n2c.c_state, ol.ol_amount, n1n2c.n1key
                     FROM orders o,
                          o.o_orderline ol
                              JOIN (SELECT c.c_id, c.c_w_id, c.c_d_id, c.c_state, n1n2.n1key
                                    FROM customer c
                                             JOIN (SELECT n1.n_nationkey n1key, n2.n_nationkey n2key
                                                   FROM nation n1,
                                                        nation n2
                                                   WHERE (n1.n_name = 'Germany' AND n2.n_name = 'Cambodia')
                                                      OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany')) n1n2
                                                  ON string_to_codepoint(c.c_state)[0] = n1n2.n2key) n1n2c
                                   ON n1n2c.c_id = o.o_c_id AND n1n2c.c_w_id = o.o_w_id AND n1n2c.c_d_id = o.o_d_id AND
                                      ol.ol_delivery_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000') n1n2cool
                    ON n1n2cool.ol_supply_w_id = s.s_w_id AND n1n2cool.ol_i_id = s.s_i_id) n1n2cools
         JOIN supplier su ON n1n2cools.s_w_id * n1n2cools.s_i_id MOD 10000 = su.su_suppkey AND su.su_nationkey = n1n2cools.n1key
GROUP BY su.su_nationkey, SUBSTR1(n1n2cools.c_state, 1, 1), DATE_PART_STR(n1n2cools.o_entry_d, 'year')
ORDER BY su.su_nationkey, cust_nation, l_year
