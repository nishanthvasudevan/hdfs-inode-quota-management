#!/bin/bash


hdfspath=$1

if [ ${hdfspath}xx == xx ]; then
	echo "Usage $0 <hdfs path> . e.g., $0 /user/nishanth.vasudevan"
	exit 1
fi
sudo -u hdfs hadoop fsck "${hdfspath}" -files -blocks -locations > /tmp/$$.1 2> /dev/null
./getZeroByteFiles.py -f /tmp/$$.1
rm -f /tmp/$$.1
