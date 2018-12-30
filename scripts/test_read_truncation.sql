

create table hive.tpch.strings as select
 orderkey, linenumber, comment as s1,
 concat (cast(partkey as varchar), comment) as s2,
        concat(cast(suppkey as varchar), comment) as s3,
                            concat(cast(quantity as varchar), comment) as s4
                            from hive.tpch.lineitem_s where orderkey < 100000;

select orderkey, linenumber, s1, s2, s3, s4 from hive.tpch.strings where
 s1 > 'f'
 and s2 > '1'
 and s3 > '1'
 and s4 > '2';
 


-- Example of filter reorder gains, from 58s cpu to 42s cpu
select count (*) from hive.tpch.lineitem_s where partkey < 19000000 and suppkey < 900000 and quantity < 45 and extendedprice < 9000;
