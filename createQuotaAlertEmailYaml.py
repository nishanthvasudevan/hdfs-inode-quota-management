#!/usr/bin/env python

import sys

import ast
import yaml

#/projects/localdc	grid-alerts@inmobi.com,bi-dev@inmobi.com
dict1 = {}
workingDir = ["/user","/data","/project","/projects"]
with open('gold_inode_quota.csv','r') as emaillistfd:
	for line in emaillistfd:
		dir = line.split('\t')[0]
		if not line.split('\t')[-1].strip():
			continue
		emails = line.split('\t')[-1].strip().split(",")
		emails.append("grid-alerts@inmobi.com")
		parent = "/{}".format(dir.split("/")[1])
		if parent not in workingDir:
			continue
		child = dir.split("/")[2]
		if parent in dict1.keys():
			dict1[parent][child] = emails
		else:
			dict2 = {}
			dict2[child] = emails
			dict1[parent] = dict2
		

with open('nameQuota-alert-email.yaml','w') as yaml_file:
        yaml_file.write(yaml.dump(dict1, default_flow_style=False))
