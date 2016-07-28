#!/bin/bash

cluster=$(facter hadoop_conf)

./getQuotaDetails.py > /tmp/$$.1

< /tmp/$$.1 awk -F"\t" '{print $NF"\t"$(NF-1)"\t"$3"\t"$4"\t"$1"\t"$2"\t"$5"\t"$6"\t"$5+$6}' > ${cluster}.quota

rm -f /tmp/$$.1
