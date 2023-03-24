
add-file:
	docker cp data/Q9Y261.xml hadoop-namenode:/
	docker exec hadoop-namenode bash -c hadoop dfs -mkdir -p hdfs:///data/landing/xmlfile/
	docker exec hadoop-namenode bash -c "hadoop fs -copyFromLocal /trips.csv hdfs:///data/landing/xmlfile/Q9Y261.xml"
