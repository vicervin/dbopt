-- using 1512813808 as a seed to the RNG

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue.value
from
    supplier,
    (select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as value
    from
        lineitem
    where
        l_shipdate >= date '1995-10-01'
        and l_shipdate < date '1995-10-01' + interval '3' month
    group by
        l_suppkey
    order by
        value desc
    limit 1) total_revenue
where
    s_suppkey = total_revenue.supplier_no
order by
    s_suppkey;
