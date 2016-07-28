#!/usr/bin/env python

import sys
import time
import yaml
import subprocess
import ast
from optparse import OptionParser
from datetime import datetime

def getCurrentNameQuota(nameQuotaDictionary, cluster):
	print "{}, fetching current nameQuota from hdfs://{}".format(datetime.now(), cluster)
	usage = "{"
	for key in nameQuotaDictionary[cluster]:
        	if key.startswith("/"):
                	parentHdfsDirectory = key
			cmd = 'sudo -u hdfs hdfs dfs -count -q "{}/*"'.format(parentHdfsDirectory)
			currentUsageList = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].rstrip().split('\n')
			if len(currentUsageList) == 1 and currentUsageList[0] == "":
				continue
			usage += "'{}':".format(parentHdfsDirectory)
			usage += "{"
			for listitem in currentUsageList:
				childHdfsDirectory = listitem.split()[-1].strip().split('/')[-1]
				currentNameQuota = listitem.split()[0].strip()
				if currentNameQuota == "none":
					currentNameQuota = None
				else:
					currentNameQuota = int(currentNameQuota)
				usage += "'{}':{},".format(childHdfsDirectory, currentNameQuota)
			temp = usage.rsplit(",",1)
			usage = ''.join(temp)
			usage += "},"
	temp = usage.rsplit(",",1)
	usage = ''.join(temp)
	usage += "}"
	usageDict = ast.literal_eval(usage)
	return usageDict

def setNameQuota(hdfsDirectory, nameQuota, cluster):
	cmd = 'sudo -u hdfs hdfs dfsadmin -setQuota {} "{}"'.format(nameQuota, hdfsDirectory)
	t1 = time.time()
	setQuotaCmd = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)	
	retval = setQuotaCmd.wait()
	t2 = time.time()
	if retval != 0:
		print "{}, {} failed in hdfs://{}, time taken = {:.2f} seconds".format(datetime.now(), cmd, cluster, t2-t1)
	else:
		print "{}, {} succeeded in hdfs://{}, time taken = {:.2f} seconds".format(datetime.now(), cmd, cluster, t2-t1)

def clrNameQuota(hdfsDirectory, cluster):
	cmd = 'sudo -u hdfs hdfs dfsadmin -clrQuota "{}"'.format(hdfsDirectory)
	t1 = time.time()
	clrQuotaCmd = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)	
	retval = clrQuotaCmd.wait()
	t2 = time.time()
	if retval != 0:
		print "{}, {} failed in hdfs://{}, time taken = {:.2f} seconds".format(datetime.now(), cmd, cluster, t2-t1)
	else:
		print "{}, {} succeeded in hdfs://{}, time taken = {:.2f} seconds".format(datetime.now(), cmd, cluster, t2-t1)


def updateNameQuota(currentNameQuotaDictionary, newNameQuotaDictionary, cluster):
	'''
	new -> nameQuotaDictionary[cluster][parentHdfsDirectory]['exceptions'][childHdfsDirectory] yaml to dict
	current -> {'/data':{'lda': 'none', 'staging': 'none'}, '/user':{'ashish.bhat': 'none', 'waseema': '1500', 'prathik.raj': 'none'}}
	'''
	seen = set()
	for parentHdfsDirectory in currentNameQuotaDictionary.keys():
		if parentHdfsDirectory in newNameQuotaDictionary[cluster].keys():
			if "exceptions" in newNameQuotaDictionary[cluster][parentHdfsDirectory].keys():
				for childHdfsDirectory in newNameQuotaDictionary[cluster][parentHdfsDirectory]["exceptions"]:
					seen.add(childHdfsDirectory)
					try:
						currentNameQuota = currentNameQuotaDictionary[parentHdfsDirectory][childHdfsDirectory]
					except KeyError:
						print "{}, {}/{} does not exist in hdfs://{}".format(datetime.now(), parentHdfsDirectory, childHdfsDirectory, cluster)
						continue
					newNameQuota = newNameQuotaDictionary[cluster][parentHdfsDirectory]["exceptions"][childHdfsDirectory]
					if newNameQuota is None:
						print "{}, Clearing NameQuota for hdfs://{}{}/{}".format(datetime.now(), cluster, parentHdfsDirectory, childHdfsDirectory)
						hdfsDirectory = "{}/{}".format(parentHdfsDirectory, childHdfsDirectory)
						clrNameQuota(hdfsDirectory, cluster)
						continue
					if currentNameQuota is None or currentNameQuota != newNameQuota:
						hdfsDirectory = "{}/{}".format(parentHdfsDirectory, childHdfsDirectory)
						setNameQuota(hdfsDirectory, newNameQuota, cluster)
			for childHdfsDirectory in currentNameQuotaDictionary[parentHdfsDirectory]:
				if childHdfsDirectory in seen:
					continue
				newNameQuota = newNameQuotaDictionary[cluster][parentHdfsDirectory]["default"]
				currentNameQuota = currentNameQuotaDictionary[parentHdfsDirectory][childHdfsDirectory]
				if currentNameQuota is None or currentNameQuota != newNameQuota:
					hdfsDirectory = "{}/{}".format(parentHdfsDirectory, childHdfsDirectory)
					setNameQuota(hdfsDirectory, newNameQuota, cluster)


def main():
	parser = OptionParser(usage="Usage: %prog [options]", version="%prog 1.0")
	parser.add_option("-f", "--config-file", action="store", dest="nameQuotaYamlFile", default="set-hdfs-name-quota.yaml", help="nameQuota value for this directory")

	(options, args) = parser.parse_args()

	nameQuotaYamlFile = options.nameQuotaYamlFile

	cluster = subprocess.Popen("/usr/bin/facter hadoop_conf", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()

	with open(nameQuotaYamlFile, "r") as nameQuotaYamlFileDescriptor:
		newNameQuotaDictionary = yaml.load(nameQuotaYamlFileDescriptor)
	
	currentNameQuotaDictionary = getCurrentNameQuota(newNameQuotaDictionary, cluster)
	updateNameQuota(currentNameQuotaDictionary, newNameQuotaDictionary, cluster)

if __name__ == '__main__':
	main()
