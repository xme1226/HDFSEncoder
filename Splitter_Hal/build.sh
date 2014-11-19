javac -cp /home/hadoop/Splitter_Hal/joss-0.9.9-SNAPSHOT-jar-with-dependencies.jar:/opt/hadoop-2.4.1/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/mapreduce/lib/*:/opt/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar hdfsutil_sp.java Splitter.java
jar -cvfm Splitter.jar MANIFEST.MF -C . .

