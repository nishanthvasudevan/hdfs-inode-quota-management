#!/usr/bin/env python

import sys
import subprocess
import yaml

'''
gold:
  NAMENODE: gold 
  HDFS_PORT: 
  /user: 
     default: 1500
     exceptions:
           yoda: 2473901162496000
           rmcuser: 3298534883328


/projects/rtbd  10670   10670   0.009119284385  13699   3029    dcp-engg@inmobi.com
/projects/rtbs  102     1500    0.001281998742  1500    0       tushar.ghosh@inmobi.com,seema.saroha@inmobi.com,pso-central@inmobi.com
/projects/rtes  1       1500    0.001281998742  1500    0

'''

dict0 = {}
dict1 = {}
workingDir = ["/user","/data","/project","/projects","/user"]
cluster = subprocess.Popen("/usr/bin/facter hadoop_conf", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()

dict1["NAMENODE"] = cluster
dict1["HDFS_PORT"] = None

with open('gold_inode_quota.csv','r') as fd:
	for line in fd:
		dir = line.split('\t')[0]
		quota = unicode(line.split('\t')[4], 'utf-8')
		if quota.isnumeric():
			quota = int(quota)
		else:
			quota = None
		if quota == 1500:
			continue
		parent = "/{}".format(dir.split("/")[1])
		child = dir.split("/")[2]
		if parent not in workingDir:
			continue
		if parent in dict1.keys():
			dict1[parent]["exceptions"][child] = quota
		else:
			dict2 = {}
			dict3 = {}
			dict3[child] = quota
			dict2["default"] = 1500
			dict2["exceptions"] = dict3
			dict1[parent] = dict2
		
dict0[cluster]= dict1
print dict0
with open('set-hdfs-name-quota.yaml','w') as yaml_file:
        yaml_file.write(yaml.dump(dict0, default_flow_style=False))




with open("set-hdfs-name-quota.yaml", "r") as nameQuotaYamlFileDescriptor:
	newNameQuotaDictionary = yaml.load(nameQuotaYamlFileDescriptor)
print newNameQuotaDictionary
