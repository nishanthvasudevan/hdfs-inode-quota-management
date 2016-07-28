#!/bin/bash

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/opt/inmobi/infra/bin:/usr/lib/jvm/java-8-oracle/bin:/usr/lib/jvm/java-8-oracle/db/bin:/usr/lib/jvm/java-8-oracle/jre/bin:/usr/local/inmobi/bin:/usr/hdp/current/hadoop/bin:/usr/hdp/current/hadoop-client/bin:/opt/gridscripts/gridopstools/bin:/usr/local/spark/bin:/usr/local/pig-spork/bin:/opt/inmobi/sysadmin/bin:/opt/inmobi/sysadmin/bin/remotescripts:/usr/local/inmobi-hive/bin/

export JAVA_HOME=/usr/lib/jvm/current

export CLASSPATH=/usr/hdp/2.2.4.2-2/hadoop/conf:/usr/hdp/2.2.4.2-2/hadoop/lib/*:/usr/hdp/2.2.4.2-2/hadoop/.//*:/usr/hdp/2.2.4.2-2/hadoop-hdfs/./:/usr/hdp/2.2.4.2-2/hadoop-hdfs/lib/*:/usr/hdp/2.2.4.2-2/hadoop-hdfs/.//*:/usr/hdp/2.2.4.2-2/hadoop-yarn/lib/*:/usr/hdp/2.2.4.2-2/hadoop-yarn/.//*:/usr/hdp/2.2.4.2-2/hadoop-mapreduce/lib/*:/usr/hdp/2.2.4.2-2/hadoop-mapreduce/.//*::/usr/share/java/jdbc-mysql.jar:/usr/share/java/mysql-connector-java-5.1.29-bin.jar:/usr/share/java/mysql-connector-java.jar:/usr/hdp/current/hadoop-mapreduce-client/*:/usr/local/spark-1.5.1-yarn-shuffle.jar:/usr/hdp/2.2.4.2-2/tez/*:/usr/hdp/2.2.4.2-2/tez/lib/*:/etc/tez/conf:.

cluster=$(facter hadoop_conf)
hdfsPath=$1

curdir=`pwd`
cd /opt/gridscripts/gridopstools/jar

sudo -u hdfs java RecursivelyPrintZeroByteFilesOnHDFS $cluster $hdfsPath 2> /dev/null

cd $curdir
