use hive.tpch;

-- aria enable Aria scan. Works with ORC V2 direct longs, doubles and direct Slices
set session aria=true;
-- OR of 1: enable repartitioning 2: enable hash join, works only with fixed size data, hash join w 2 longs k=as key and 1 double as payload.
set session aria_flags = 3;
-- Enables reusing Pages and Blocks between result batches if the pipeline is suitable, e.g. scan - repartition - hash join
set session aria_reuse_pages = true;
-- Enables adaptive reorder of single column filters.
set session aria_reorder = true;


-- The literals are scaled  for 100G scale. The tables should be compressed with Snappy.

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
