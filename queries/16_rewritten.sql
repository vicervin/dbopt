-- using 1512813808 as a seed to the RNG

select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#32'
    and p_type not like 'LARGE BRUSHED%'
    and p_size in (44, 41, 13, 46, 36, 43, 31, 33)
    and not exists (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%' and ps_suppkey = s_suppkey
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;
