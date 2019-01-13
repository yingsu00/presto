

-- custkey with map from year to an array of purchases that year.


create table hive.tpch.cust_year_parts as select custkey, map_agg(y, parts) as year_parts, map_agg(y, total_cost) as year_cost
  from (select c.custkey, year(shipdate) as y, array_agg(cast (row (partkey, extendedprice, quantity) as row (pk bigint, ep double, qt double))) as parts, sum (extendedprice) as total_cost
  from hive.tpch.lineitem_s l, hive.tpch.orders o, hive.tpch.customer c where l.orderkey = o.orderkey and o.custkey = c.custkey and c.nationkey = 1 and quantity < 10
  group by c.custkey, year(shipdate))
  group by custkey;



  

create table exportinfo as
select l.orderkey, linenumber, l.partkey, l.suppkey, extendedprice, discount, quantity, shipdate, receiptdate, commitdate, l.comment,
  CASE WHEN S.nationkey = C.nationkey THEN NULL ELSE 
CAST (row(
S.NATIONKEY, C.NATIONKEY,
CASE WHEN (S.NATIONKEY IN (6, 7, 19) AND C.NATIONKEY IN (6,7,19)) THEN 1 ELSE 0 END,
case when s.nationkey = 24 and c.nationkey = 10 then 1 else 0 end,
 case when p.comment like '%fur%' or p.comment like '%care%'
 then row(o.orderdate, l.shipdate, l.partkey + l.suppkey, concat(p.comment, l.comment))
   else null end
)
AS ROW (
S_NATION BIGINT, C_NATION BIGINT,
IS_INSIDE_EU INT,
IS_RESTRICTED INT,
LICENSE ROW (APPLYDATE DATE, GRANTDATE DATE, FILING_NO BIGINT, COMMENT VARCHAR)))
END AS EXPORT
FROM LINEITEM L, ORDERs O, CUSTOMER C, SUPPLIER S, PART P
WHERE L.ORDERKEY = O.ORDERKEY AND L.PARTKEY = P.PARTKEY AND L.SUPPKEY = S.SUPPKEY AND C.CUSTKEY = O.CUSTKEY 
AND L.ORDERKEY < 1000000;


select l.applydate from (select e.license as l from (select export as e from hive.tpch.exportinfo where orderkey < 5));

