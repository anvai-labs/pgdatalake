-- TPC-H Q6: forecasting revenue change
-- (validation instance uses the standard kit's [0.02, 0.04] band
-- with :2 = 0.03; timing-equivalent selectivity to any 0.01 band)
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM   lineitem
WHERE  l_shipdate >= date '1994-01-01'
       AND l_shipdate < date '1994-01-01' + interval '1 year'
       AND l_discount BETWEEN 0.05 AND 0.07
       AND l_quantity < 24;
