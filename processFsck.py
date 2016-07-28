#!/usr/bin/env python

import sys
import re
import subprocess
import threading
from optparse import OptionParser

inodeDistributionByBlock = {}
fileSizeDistribution = {}
replicationDistribution = {}
pctsList = []
pctsDict = {}
interval10_100 = {1:(0,0), 2:(0,0), 3:(0,0), 4:(0,0), 5:(0,0), 6:(0,0), 7:(0,0), 8:(0,0), 9:(0,0), 10:(0,0)}
interval0_9 = {0:(0,0), 1:(0,0), 2:(0,0), 3:(0,0), 4:(0,0), 5:(0,0), 6:(0,0), 7:(0,0), 8:(0,0), 9:(0,0)}

def processFileSizeAndBlocks(matchedBlock, maxBlockSize):
	fileSize = int(matchedBlock.group(1))
	numBlocks = matchedBlock.group(2)
	if numBlocks in inodeDistributionByBlock.keys():
		#lock
		fileCount,totalSize = inodeDistributionByBlock[numBlocks]
		fileCount += 1
		totalSize += fileSize
		inodeDistributionByBlock[numBlocks] = (fileCount,totalSize)
		#unlock
		if int(numBlocks) == 1:
			pctOfMaxBlockSize = int(float(fileSize) / maxBlockSize * 100)
			interval = int(pctOfMaxBlockSize / 10)
			if interval == 0:
				interval = int(pctOfMaxBlockSize / 1)
				#lock
				fileCount, totalSize = interval0_9[interval]
				fileCount += 1
				totalSize += fileSize
				interval0_9[interval] = (fileCount, totalSize)
				#unlock
			elif interval > 9:
				#lock
				fileCount, totalSize = interval10_100[10]
				fileCount += 1
				totalSize += fileSize
				interval10_100[10] = (fileCount, totalSize)
				#unlock
			else:
				#lock
				fileCount, totalSize = interval10_100[interval]
				fileCount += 1
				totalSize += fileSize
				interval10_100[interval] = (fileCount, totalSize)
				#unlock
	else:
		#lock
		inodeDistributionByBlock[numBlocks] = (0,0)
		#unlock
	

def processReplicationDetails(matchedBlock):
	#replicationFactor = matchedBlock.group(1)
	locations = matchedBlock.group(2).split(",")
	for location in locations:
		host = location.replace(":50010","").strip()
		if host in replicationDistribution.keys():
			replicationDistribution[host] += 1
		else:
			replicationDistribution[host] = 1
	

def generatePcts(maxBlockSize, distributionList):
	for pct in distributionList:
		pctOfmaxBlockSize = int(maxBlockSize * int(pct) / 100)
		pctsList.append(pctOfmaxBlockSize)
		pctsDict[pctOfmaxBlockSize] = pct
	pctsList.sort(reverse=True)

def genReport():
	#print inodeDistributionByBlock['1']
	t = 0 
	c = 0
	for key,value in interval0_9.items():
		count,size = value
		t += size
		c += count
	for key,value in interval10_100.items():
		count,size = value
		t += size
		c += count
	#print "Count of 1 block files = {}, Total size of 1 block files = {}".format(c, t)
	tall = 0
	call = 0
	for key,value in inodeDistributionByBlock.items():
		count,size = value
		tall += size
		call += count
	print "Total number of files = {}, Total Size = {}".format(call, tall)
	sortedinodeDistributionByBlock = sorted(inodeDistributionByBlock.iteritems(), key=lambda (k,v): int(k))
	totalFileCount = 0
	for key,value in sortedinodeDistributionByBlock:
		bcount, tsize = value
		totalFileCount += bcount
	for key,value in sortedinodeDistributionByBlock:
		bcount, tsize = value
		print "Count of {} block(s) files = {}, Percentage of {} block(s) files = {:.2f}%, Total Size = {}".format(key, bcount, key, float(bcount)/totalFileCount * 100, tsize)
	'''	
	c, t = interval0_9[0]
	print "Total number of 1 block files that are between 0-1% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval0_9[1]
	print "Total number of 1 block files that are between 1-2% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval0_9[2]
	print "Total number of 1 block files that are between 2-3% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval0_9[3]
	print "Total number of 1 block files that are between 3-4% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval0_9[4]
	print "Total number of 1 block files that are between 4-5% of 128MB = {}, Total size = {}".format(c, t)
	c10 = 0
	t10 = 0
	c, t = interval0_9[5]
	c10 += c
	t10 += t
	c, t = interval0_9[6]
	c10 += c
	t10 += t
	c, t = interval0_9[7]
	c10 += c
	t10 += t
	c, t = interval0_9[8]
	c10 += c
	t10 += t
	c, t = interval0_9[9]
	c10 += c
	t10 += t
	print "Total number of 1 block files that are between 5-10% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[1]
	print "Total number of 1 block files that are between 10-20% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[2]
	print "Total number of 1 block files that are between 20-30% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[3]
	print "Total number of 1 block files that are between 30-40% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[4]
	print "Total number of 1 block files that are between 40-50% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[5]
	print "Total number of 1 block files that are between 50-60% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[6]
	print "Total number of 1 block files that are between 60-70% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[7]
	print "Total number of 1 block files that are between 70-80% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[8]
	print "Total number of 1 block files that are between 80-90% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[9]
	print "Total number of 1 block files that are between 90-100% of 128MB = {}, Total size = {}".format(c, t)
	c, t = interval10_100[10]
	print "Total number of 1 block files that are greater than 128MB = {}, Total size = {}".format(c, t)
	'''
	#sortedReplicationDistribution = sorted(replicationDistribution.iteritems(), key=lambda (k,v): v, reverse=True)
	#print "Count of blocks per datanode reverse numerically sorted"
	#for key,value in sortedReplicationDistribution:
        #	print "{}	{}".format(key,value)
	
		
def main():

	cluster = subprocess.Popen("/usr/bin/facter hadoop_conf", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()

	parser = OptionParser(usage="Usage: %prog [options]", version="%prog 1.0")	
	parser.add_option("-f", "--fsck-dump", action="store", dest="fsckDump", default="{}.fsck".format(cluster), help="hadoop fsck dump")
	#parser.add_option("-d", "--distribution", action="store", dest="distribution", default="1,2,3,4,5,10,20,30,40,50,60,70,80,90", help="Distribution separated by comma, to generate histogram")
	parser.add_option("-m", "--max-blocksize", action="store", dest="maxBlockSize", default=134217728, help="Max blocksize of HDFS")
	
	(options, args) = parser.parse_args()
	fsckDump = options.fsckDump
	#distribution = options.distribution.split(",")
	maxBlockSize = options.maxBlockSize
	#generatePcts(maxBlockSize, distribution)
	spaceBlockInfoLinePattern = re.compile(r'(\d+) bytes, (\d+) block\(s\):')
	replicationInfoLinePattern = re.compile(r'repl=(\d+) \[(.*)\]')
	

	with open(fsckDump,'r') as fsckDumpfd:
		for rawline in fsckDumpfd:
        		spaceBlockInfoLinePatternMatch = spaceBlockInfoLinePattern.search(rawline.strip())
        		replicationInfoLinePatternMatch = replicationInfoLinePattern.search(rawline.strip())
			if spaceBlockInfoLinePatternMatch:
				processFileSizeAndBlocks(spaceBlockInfoLinePatternMatch, maxBlockSize)	
			#if replicationInfoLinePatternMatch:
			#	processReplicationDetails(replicationInfoLinePatternMatch)
	genReport()
	
if __name__ == '__main__':
	main()	
	
