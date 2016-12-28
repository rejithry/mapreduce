cd /code/testjava8/
mvn package

cd /usr/local/hadoop
bin/hadoop fs -copyFromLocal /top10.txt / || true
bin/hadoop fs -copyFromLocal /url.txt / || true
bin/hadoop fs -rm -r /testoutput || true
bin/hadoop fs -rm -r /tmp/pagerank || true

bin/hadoop jar /code/testjava8/target/test8-1.0-SNAPSHOT-jar-with-dependencies.jar com.test.PageRank /url.txt /testoutput

