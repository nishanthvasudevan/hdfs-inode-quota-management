#!/usr/bin/env python

import sys
import subprocess
import re
from optparse import OptionParser

def listHdfsDir(dir):
	'''
	Lists all files and directories under the given directory
	'''
	dirList = []
	cmd = 'sudo -u hdfs hadoop fs -ls "{}"'.format(dir)
	cmdOutputList = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip().split("\n")
	
	for cmdOutput in cmdOutputList:
		if cmdOutput.startswith("Found"):
			continue
		dirList.append(cmdOutput.rsplit(None, 1)[1])
	return dirList

def getQuotaDetails(dir):
	'''
	Prints QUOTA  REMAINING_QUOTA     SPACE_QUOTA  REMAINING_SPACE_QUOTA    DIR_COUNT  FILE_COUNT      CONTENT_SIZE FILE_NAME of all files and directories under dir, columns separated by tab (CTRL-V CTRL-I)
	'''
	cmd = 'sudo -u hdfs hdfs dfs -count -q "{}/*"'.format(dir)	
	cmdOutputList = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].rstrip().split('\n')
	for output in cmdOutputList:
		if output:
			strippedOutput = output.strip()
			print re.sub(' +','	',strippedOutput)

def main():	
	parser = OptionParser(usage="Usage: %prog [options]", version="%prog 1.0")
	parser.add_option("-s", "--skip-directory", action="store", dest="skipDirectories", default="/tmp", help="Skip directories. You can provide multiple directories by separating them by comma.")

	(options, args) = parser.parse_args()
	skipDirectories = options.skipDirectories
	skipDirList = skipDirectories.split(",")
	dirlist = listHdfsDir("/")
	
	for dir in dirlist:
		#if dir in skipDirList:
			#continue
		getQuotaDetails(dir)

if __name__ == "__main__":
	main()
