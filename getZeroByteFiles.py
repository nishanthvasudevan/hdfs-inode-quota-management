#!/usr/bin/env python

import sys
import re
import subprocess
from optparse import OptionParser


dirTree = {}

class DictQuery(dict):
	def get(self, path, default = None):
		keys = path.split("/")
		val = None
		for key in keys:
			if val:
				if isinstance(val, list):
					val = [ v.get(key, default) for v in val]
				else:
					val = val.get(key, default)
			else:
				val = dict.get(self, key, default)

			if not val:
				break
		return val 

def getZeroBlockFiles(matchedBlock, maxBlockSize, rawline):
	fileSize = int(matchedBlock.group(1))
	numBlocks = matchedBlock.group(2)
	if fileSize == 0:
		print rawline.strip()	

def depth(x):
	if type(x) is dict and x:
		return 1 + max(depth(x[a]) for a in x)
	if type(x) is list and x:
	return 1 + max(depth(a) for a in x)
	return 0

def processDir(matchedBlock, rawline):
	dirPath = matchedBlock.group(1)
	parentDir = dirPath.rsplit("/", 1)[0]
	if parentDir == '':
		dirTree[dirPath] = None
		return 0
	if type(DictQuery(dirTree).get(parentDir)) is list:
		dirTree[dirPath] = None
	if type(DictQuery(dirTree).get(parentDir)) is None:
			
	for component in dirEnt:
				
	if dirPath in dirTree.keys():
		if type(dirTree[dir]) is list:
			components = dir.split("/")
	else:
		dirTree[dir] = None


def procDir(matchedBlock, rawline):
	dirPath = matchedBlock.group(1)
	dirEnt = dirPath.split("/")	
	dirTree[

def main():

	cluster = subprocess.Popen("/usr/bin/facter hadoop_conf", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()

	parser = OptionParser(usage="Usage: %prog [options]", version="%prog 1.0")	
	parser.add_option("-f", "--fsck-dump", action="store", dest="fsckDump", default="{}.fsck".format(cluster), help="hadoop fsck dump")
	parser.add_option("-m", "--max-blocksize", action="store", dest="maxBlockSize", default=134217728, help="Max blocksize of HDFS")
	
	(options, args) = parser.parse_args()
	fsckDump = options.fsckDump
	maxBlockSize = options.maxBlockSize
	dirLinePattern = re.compile(r'(.*) <dir>')
	spaceBlockInfoLinePattern = re.compile(r'(\d+) bytes, (\d+) block\(s\):')
	replicationInfoLinePattern = re.compile(r'repl=(\d+) \[(.*)\]')
	

	with open(fsckDump,'r') as fsckDumpfd:
		for rawline in fsckDumpfd:
        		spaceBlockInfoLinePatternMatch = spaceBlockInfoLinePattern.search(rawline.strip())
        		replicationInfoLinePatternMatch = replicationInfoLinePattern.search(rawline.strip())
			dirLinePatternMatch = dirLinePattern.search(rawline.strip())
			if dirLinePatternMatch:
				
			if spaceBlockInfoLinePatternMatch:
				getZeroBlockFiles(spaceBlockInfoLinePatternMatch, maxBlockSize, rawline)
	
if __name__ == '__main__':
	main()	
	
