javac -cp /opt/hadoop-2.3.0/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/mapreduce/lib/*:/opt/hadoop/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar converter_me.java hdfsutil_sp.java Merge.java
#jar -cvfm Merge.jar META-INF/MANIFEST.MF -C . .
jar -cvfm Merge.jar MANIFEST.MF -C . .
