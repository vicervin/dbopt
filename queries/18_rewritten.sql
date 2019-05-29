-- using 1512813808 as a seed to the RNG

alter system set work_mem = '1GB';
select pg_reload_conf();

select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem l,
    (select
        l_orderkey
    from
        lineitem
    group by
        l_orderkey having sum(l_quantity) > 314) as tmp
where
    o_orderkey = tmp.l_orderkey
    and c_custkey = o_custkey
    and o_orderkey = l.l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;

alter system reset work_mem;
select pg_reload_conf();
