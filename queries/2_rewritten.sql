-- using 1512813808 as a seed to the RNG

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region,
    (
        select
            min(ps_supplycost) as ps_min_supplycost, ps_partkey as ps_grouped_partkey
        from
            partsupp,
            supplier,
            nation,
            region
        where
            s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'AMERICA'
        group by ps_partkey
    ) tmp
where
    p_partkey = ps_partkey
    and p_partkey = ps_grouped_partkey
    and s_suppkey = ps_suppkey
    and p_size = 41
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AMERICA'
    and ps_supplycost = ps_min_supplycost
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
