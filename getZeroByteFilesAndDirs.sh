#!/bin/bash

cluster=$(facter hadoop_conf)
hdfsPath=$1

curdir=`pwd`

sudo -u hdfs java RecursivelyPrintZeroByteFilesOnHDFS $cluster $hdfsPath 2> /dev/null

cd $curdir
