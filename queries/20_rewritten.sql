-- using 1512813808 as a seed to the RNG

SELECT
    s_name
    ,s_address
FROM
    supplier,
    nation,
    (SELECT
        ps_suppkey
    FROM
        partsupp,
        (SELECT
            l_suppkey, l_partkey, 0.5 * sum(l_quantity) as l_quantity_sum
        FROM
            lineitem,
            part
        WHERE
            l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1994-01-01' + interval '1' year        
            AND p_partkey = l_partkey
            AND p_name LIKE 'salmon%'
        GROUP BY
            l_partkey,
            l_suppkey
        ) tmp
    WHERE
        ps_availqty > tmp.l_quantity_sum
        AND tmp.l_partkey = ps_partkey
        AND tmp.l_suppkey = ps_suppkey) tmp_outer
WHERE
    s_suppkey = tmp_outer.ps_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'RUSSIA'
ORDER BY
    s_name;
