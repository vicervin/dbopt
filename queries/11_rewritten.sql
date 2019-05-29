-- using 1512813808 as a seed to the RNG

select
    *
from
    (
    select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
    from
        partsupp,
        supplier,
        nation
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'ROMANIA'
    group by
        ps_partkey
    ) tmp_grouped,
    (
    select
        sum(ps_supplycost * ps_availqty) * 0.0001000000 as value
    from
        partsupp,
        supplier,
        nation
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'ROMANIA'
    ) tmp_fraction
where
    tmp_grouped.value > tmp_fraction.value
order by
    tmp_grouped.value desc;
