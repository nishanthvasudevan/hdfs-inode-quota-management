#!/usr/bin/env python

import sys
import time
import yaml
import subprocess
import socket
import ast
from optparse import OptionParser
from datetime import datetime

def getCurrentNameQuota(nameQuotaDictionary, colo):
	usage = "{"
	for key in nameQuotaDictionary[colo]:
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
				remainingNameQuota = listitem.split()[1].strip()
				dirCount = listitem.split()[4].strip().strip()
				fileCount = listitem.split()[5].strip().strip()
				if currentNameQuota == "none":
					currentNameQuota = None
				else:
					currentNameQuota = int(currentNameQuota)
				if remainingNameQuota == "inf":
					remainingNameQuota = None
				else:
					remainingNameQuota = int(remainingNameQuota)
				inodeCount = int(dirCount) + int(fileCount)
				usage += "'{}':[{},{},{}],".format(childHdfsDirectory, currentNameQuota, remainingNameQuota, inodeCount)
			temp = usage.rsplit(",",1)
			usage = ''.join(temp)
			usage += "},"
	temp = usage.rsplit(",",1)
	usage = ''.join(temp)
	usage += "}"
	usageDict = ast.literal_eval(usage)
	return usageDict

def emitMetricsToGraphite(graphiteHost, graphitePort, nameQuotaUsageMetrics):
	host = graphiteHost
	port = graphitePort
	s = socket.socket()
	s.connect((host,port))
	s.sendall('\n'.join(nameQuotaUsageMetrics) + '\n')
	s.close()

def getNameQuotaUsageMetrics(currentNameQuotaDictionary, newNameQuotaDictionary, cluster, graphitePrefix):
	now = time.time()
	nameQuotaUsageMetrics = []
	for parentHdfsDirectory in currentNameQuotaDictionary.keys():
		for childHdfsDirectory in currentNameQuotaDictionary[parentHdfsDirectory]:
			maxNameQuota = currentNameQuotaDictionary[parentHdfsDirectory][childHdfsDirectory][0]
			inodeCount = currentNameQuotaDictionary[parentHdfsDirectory][childHdfsDirectory][2]
			if maxNameQuota == None:
				maxNameQuota = 150000000
			nameQuotaUsageMetrics.append("{}.{}.{}.maxNameQuota {} {}".format(graphitePrefix, parentHdfsDirectory[1:], childHdfsDirectory.replace(".","-"), maxNameQuota, now))
			nameQuotaUsageMetrics.append("{}.{}.{}.usedNameQuota {} {}".format(graphitePrefix, parentHdfsDirectory[1:], childHdfsDirectory.replace(".","-"), inodeCount, now))
	return nameQuotaUsageMetrics
			

def main():

	colo = subprocess.Popen("/usr/bin/facter colo", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()
	defaultGraphiteHost = ""
	cluster = subprocess.Popen("/usr/bin/facter hadoop_conf", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()
	defaultGraphitePrefix = "prod.{}.grid.{}_{}.namequota".format(colo,colo.upper(),cluster.upper())

	parser = OptionParser(usage="Usage: %prog [options]", version="%prog 1.0")
	parser.add_option("-f", "--config-file", action="store", dest="nameQuotaYamlFile", default="set-hdfs-name-quota.yaml", help="nameQuota value for this directory")
	parser.add_option("-c", "--graphite-host", action="store", dest="graphiteHost", default=defaultGraphiteHost, help="Graphite Hostname")
	parser.add_option("-p", "--graphite-port", action="store", dest="graphitePort", default=2020, help="Graphite Port")
	parser.add_option("-n", "--graphite-prefix", action="store", dest="graphitePrefix", default=defaultGraphitePrefix, help="Graphite Prefix")

	(options, args) = parser.parse_args()

	nameQuotaYamlFile = options.nameQuotaYamlFile
	graphiteHost = options.graphiteHost
	graphitePort = options.graphitePort
	graphitePrefix = options.graphitePrefix

	with open(nameQuotaYamlFile, "r") as nameQuotaYamlFileDescriptor:
		newNameQuotaDictionary = yaml.load(nameQuotaYamlFileDescriptor)
	
	currentNameQuotaDictionary = getCurrentNameQuota(newNameQuotaDictionary, cluster)
	nameQuotaUsageMetrics = getNameQuotaUsageMetrics(currentNameQuotaDictionary, newNameQuotaDictionary, cluster, graphitePrefix)
	emitMetricsToGraphite(graphiteHost, graphitePort, nameQuotaUsageMetrics)

if __name__ == '__main__':
	main()
