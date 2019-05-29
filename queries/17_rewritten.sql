-- using 1512813808 as a seed to the RNG

SELECT
    SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l,
    (SELECT
        l_partkey, 0.2 * AVG(l_quantity) AS avg_w_quantity
    FROM
        lineitem, part
    WHERE
        l_partkey = p_partkey
        AND p_brand = 'Brand#24'
        AND p_container = 'WRAP JAR'
    GROUP BY
        l_partkey) tmp
WHERE
    l.l_partkey = tmp.l_partkey
    AND l.l_quantity < tmp.avg_w_quantity;
