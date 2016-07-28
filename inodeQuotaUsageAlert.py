#!/usr/bin/env python

import sys
import time
import yaml
import subprocess
import socket
import ast
import smtplib
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
				if currentNameQuota == "none":
					currentNameQuota = None
				else:
					currentNameQuota = int(currentNameQuota)
				if remainingNameQuota == "inf":
					remainingNameQuota = None
				else:
					remainingNameQuota = int(remainingNameQuota)
				usage += "'{}':[{},{}],".format(childHdfsDirectory, currentNameQuota, remainingNameQuota)
			temp = usage.rsplit(",",1)
			usage = ''.join(temp)
			usage += "},"
	temp = usage.rsplit(",",1)
	usage = ''.join(temp)
	usage += "}"
	usageDict = ast.literal_eval(usage)
	return usageDict

def sendmail(message, toaddr, cluster, parentHdfsDirectory, childHdfsDirectory, fromaddr='sender@domain.com'):
        mailbody = message
	tmp = toaddr.replace("@domain.com","")
	tmp = tmp.replace(".", " ")
	name = tmp.title()
	msg = """From: Sender <{}>
To: {} <{}>
MIME-Version: 1.0
Content-type: text/html
Subject: HDFS://{}{}/{} NameQuota Alert

{}""".format(fromaddr, name, toaddr, cluster, parentHdfsDirectory, childHdfsDirectory, message)
        try:
                s = smtplib.SMTP('localhost')
                s.sendmail(fromaddr,toaddr,msg)
                s.quit()
        except:
                print "Exception in sending email"


def checkNameQuotaUsage(currentNameQuotaDictionary, contactsDictionary, cluster, critical):
	for parentHdfsDirectory in currentNameQuotaDictionary.keys():
		for childHdfsDirectory in currentNameQuotaDictionary[parentHdfsDirectory]:
			maxNameQuota = currentNameQuotaDictionary[parentHdfsDirectory][childHdfsDirectory][0]
			remainingNameQuota = currentNameQuotaDictionary[parentHdfsDirectory][childHdfsDirectory][1]
			if remainingNameQuota == None or maxNameQuota == None:
				continue
			currentUsage = float((maxNameQuota - remainingNameQuota)) / maxNameQuota * 100

			msg = "<font face='Verdana'><h2>NameQuota usage of hdfs://{}{}/{}</h2><br>Current usage is at {:.2f}% of {} (Max nameQuota set for this directory).<br><br>You can create {} more files and directories under {}/{}.<br><br>Consider purging some files and directories.<br><br>Your MR jobs may fail when the nameQuota is exhausted.<br><br>Regards,<br><br>Sender".format(cluster, parentHdfsDirectory, childHdfsDirectory, currentUsage, maxNameQuota, remainingNameQuota, parentHdfsDirectory, childHdfsDirectory)

			if currentUsage >= critical:
				try:
					for email in contactsDictionary[parentHdfsDirectory][childHdfsDirectory]:
						print "{}, Sending inode quota usage alert to {}, {}/{} usage is {:.2f}".format(datetime.now(), email, parentHdfsDirectory, childHdfsDirectory, currentUsage)
						sendmail(msg, email, cluster, parentHdfsDirectory, childHdfsDirectory)
				except KeyError:
					print "Could not find email for hdfs://{}{}/{}".format(cluster, parentHdfsDirectory, childHdfsDirectory)
				

def main():

	colo = subprocess.Popen("/usr/bin/facter colo", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()
	cluster = subprocess.Popen("/usr/bin/facter hadoop_conf", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()[0].strip()

	parser = OptionParser(usage="Usage: %prog [options]", version="%prog 1.0")
	parser.add_option("-f", "--quota-config-file", action="store", dest="nameQuotaYamlFile", default="set-hdfs-name-quota.yaml", help="nameQuota value for this directory")
	parser.add_option("-e", "--alert-config-file", action="store", dest="contactsYamlFile", default="nameQuota-alert-email.yaml", help="contacts file")
	parser.add_option("-c", "--critical", action="store", dest="critical", default=85, help="contacts file")

	(options, args) = parser.parse_args()

	nameQuotaYamlFile = options.nameQuotaYamlFile
	contactsYamlFile = options.contactsYamlFile
	critical = int(options.critical)

	with open(nameQuotaYamlFile, "r") as nameQuotaYamlFileDescriptor:
		newNameQuotaDictionary = yaml.load(nameQuotaYamlFileDescriptor)
	with open(contactsYamlFile, "r") as contactsYamlFileDescriptor:
		contactsDictionary = yaml.load(contactsYamlFileDescriptor)
	
	currentNameQuotaDictionary = getCurrentNameQuota(newNameQuotaDictionary, cluster)
	checkNameQuotaUsage(currentNameQuotaDictionary, contactsDictionary, cluster, critical)

if __name__ == '__main__':
	main()
