gcc -E -DSLICES -o t.java testhash.c
sed --in-place "s/^#.*//" t.java
mv t.java TestHash.java 
cd /home/oerling/presto/presto
mvn -Dair.check.skip-all  -pl presto-spi -Dtest=TestHash  test

