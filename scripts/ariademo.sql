use hive.tpch;
set session aria=true;
set session aria_flags = 3;
set session aria_reuse_pages = true;

-- 1/1M
select  sum (extendedprice) from lineitem_s where suppkey = 111;

-- 1/100 * 1/100
select count(*), sum (extendedprice), sum (quantity) from lineitem_s where  partkey between 10000000 and 10200000 and suppkey between 500000 and 510000; 


-- 1/5
select sum (partkey) from lineitem_s where quantity < 10;


-- 1/5 * 1/10
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000 and quantity < 10;

-- 1 * 1/10 
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;

-- 1 * 9/10 

select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 9000;
