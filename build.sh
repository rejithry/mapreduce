cd /code/testjava8/
mvn clean package

cd /usr/local/hadoop
bin/hadoop fs -copyFromLocal /top10.txt / || true
bin/hadoop fs -copyFromLocal /url.txt / || true
bin/hadoop fs -copyFromLocal /index.txt / || true
bin/hadoop fs -copyFromLocal /friends.txt / || true
bin/hadoop fs -copyFromLocal /reco.txt / || true

bin/hadoop fs -rm -r /testoutput || true
bin/hadoop fs -rm -r /tmp/pagerank || true
bin/hadoop fs -rm -r /tmp/intermediate || true


#bin/hadoop jar /code/testjava8/target/test8-1.0-SNAPSHOT-jar-with-dependencies.jar com.test.PageRank /url.txt /testoutput

#bin/hadoop jar /code/testjava8/target/test8-1.0-SNAPSHOT-jar-with-dependencies.jar com.test.InvertedIndex /index.txt /testoutput


#bin/hadoop jar /code/testjava8/target/test8-1.0-SNAPSHOT-jar-with-dependencies.jar com.test.CommonFriends /friends.txt /testoutput

bin/hadoop jar /code/testjava8/target/test8-1.0-SNAPSHOT-jar-with-dependencies.jar com.test.Recommendation /reco.txt /testoutput

