#!/usr/local/bin/python

#
# Purpose:
#
#	To load new reference association records into:
#
#		. MGI_Reference_Assoc
#
# Assumes:
#
#	That no one else is adding records to the database.
#
# Side Effects:
#
#	None
#
# Input:
#
#	A tab-delimited file in the format:
#		field 1: Object Accession ID
#		field 2: J: (J:#####)
#		field 3: Reference Association Type
#		field 4: Created By
#
# for JRS:
#
#		field 1: JRS ID
#		field 2: J:
#		field 3: Reference Type
#		ignore the rest
#
#	processing modes:
#		load - load the data
#
#		preview - perform all record verifications but do not load the data or
#		          make any changes to the database.  used for testing or to preview
#			  the load.
#
# Output:
#
#       1 BCP file:
#
#       MGI_Reference_Assoc.bcp
#
#	Diagnostics file of all input parameters and SQL commands
#	Error file
#
# History:
#
# 02/09/2006	lec
#	- new; JRS cutover
#	- can be converted for general use after JRS load...
#

import sys
import os
import string
import db
import mgi_utils
import loadlib

#globals

#
# from configuration file
#
user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
mode = os.environ['REFMODE']
inputFileName = os.environ['REFINPUTFILE']
mgiType = os.environ['REFOBJECTTYPE']
createdBy = os.environ['CREATEDBY']

DEBUG = 0		# set DEBUG to false unless preview mode is selected
bcpon = 1		# can the bcp files be bcp-ed into the database?  default is yes.

inputFile = ''		# file descriptor
diagFile = ''		# file descriptor
errorFile = ''		# file descriptor
refFile = ''		# file descriptor

diagFileName = ''	# file name
errorFileName = ''	# file name
refFileName = ''	# file name

refAssocKey = 0		# MGI_Reference_Assoc._Assoc_key
mgiTypeKey = 0
createdByKey = 0

refTypeDict = {}	# dictionary of reference association types for given object
refDict = {}		# existing MGI_Reference_Assoc records for given object

loaddate = loadlib.loaddate

def exit(status, message = None):
	# requires: status, the numeric exit status (integer)
	#           message (string)
	#
	# effects:
	# Print message to stderr and exits
	#
	# returns:
	#
 
	if message is not None:
		sys.stderr.write('\n' + str(message) + '\n')
 
	try:
		inputFile.close()
		diagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
		errorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
		diagFile.close()
		errorFile.close()
	except:
		pass

	db.useOneConnection()
	sys.exit(status)
 
def init():
	# requires: 
	#
	# effects: 
	# 1. Processes command line options
	# 2. Initializes local DBMS parameters
	# 3. Initializes global file descriptors/file names
	# 4. Initializes global keys
	#
	# returns:
	#
 
	global inputFile, diagFile, errorFile, errorFileName, diagFileName
	global refFileName, refFile
	global mgiTypeKey
	global refAssocKey, createdByKey
 
	db.useOneConnection(1)
        db.set_sqlUser(user)
        db.set_sqlPassword(passwordFileName)
 
	fdate = mgi_utils.date('%m%d%Y')	# current date
	head, tail = os.path.split(inputFileName) 
	diagFileName = tail + '.' + fdate + '.diagnostics'
	errorFileName = tail + '.' + fdate + '.error'
	refFileName = tail + '.MGI_Reference_Assoc.bcp'

	try:
		inputFile = open(inputFileName, 'r')
	except:
		exit(1, 'Could not open file %s\n' % inputFileName)
		
	try:
		diagFile = open(diagFileName, 'w')
	except:
		exit(1, 'Could not open file %s\n' % diagFileName)
		
	try:
		errorFile = open(errorFileName, 'w')
	except:
		exit(1, 'Could not open file %s\n' % errorFileName)
		
	try:
		refFile = open(refFileName, 'w')
	except:
		exit(1, 'Could not open file %s\n' % refFileName)
		
	# Log all SQL
	db.set_sqlLogFunction(db.sqlLogAll)

	# Set Log File Descriptor
	db.set_sqlLogFD(diagFile)

	diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
	diagFile.write('Server: %s\n' % (db.get_sqlServer()))
	diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))
	diagFile.write('Object Type: %s\n' % (mgiType))
	diagFile.write('Input File: %s\n' % (inputFileName))

	errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

	mgiTypeKey = loadlib.verifyMGIType(mgiType, 0, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, 0, errorFile)

	db.sql('delete from MGI_Reference_Assoc where _MGIType_key = %d ' % (mgiTypeKey) + \
		'and _CreatedBy_key = %d ' % (createdByKey), None)

def verifyMode():
	# requires:
	#
	# effects:
	#	Verifies the processing mode is valid.  If it is not valid,
	#	the program is aborted.
	#	Sets globals based on processing mode.
	#	Deletes data based on processing mode.
	#
	# returns:
	#	nothing
	#

	global DEBUG, bcpon

	if mode == 'preview':
		DEBUG = 1
		bcpon = 0
	elif mode != 'load':
		exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

def setPrimaryKeys():
	# requires:
	#
	# effects:
	#	Sets the global primary keys values needed for the load
	#
	# returns:
	#	nothing
	#

	global refAssocKey

        results = db.sql('select maxKey = max(_Assoc_key) + 1 from MGI_Reference_Assoc', 'auto')
        if results[0]['maxKey'] is None:
                refAssocKey = 1000
        else:
                refAssocKey = results[0]['maxKey']

def loadDictionaries():
	# requires:
	#
	# effects:
	#	loads global dictionaries: statusDict, logicalDBDict
	#	for quicker lookup
	#
	# returns:
	#	nothing

	global refTypeDict, refDict

	results = db.sql('select _RefAssocType_key, assocType ' + \
		'from MGI_RefAssocType where _MGIType_key = %s' % (mgiTypeKey), 'auto')
	for r in results:
		refTypeDict[r['assocType']] = r['_RefAssocType_key']

	results = db.sql('select _Object_key, _Refs_key, _RefAssocType_key ' + \
		'from MGI_Reference_Assoc where _MGIType_key = %s' % (mgiTypeKey), 'auto')
	for r in results:
		key = '%s:%s:%s' % (r['_Object_key'], r['_Refs_key'], r['_RefAssocType_key'])
		value = r
		refDict[key] = value

def verifyRefAssocType(refAssocType, lineNum):
	# requires:
	#	refAssocType - the Synonym Type
	#	lineNum - the line number of the record from the input file
	#
	# effects:
	#	verifies that:
	#		the Synonym Type exists 
	#	writes to the error file if the Synonym Type is invalid
	#
	# returns:
	#	0 if the Synonym Type is invalid
	#	Synonym Type Key if the Synonym Type is valid
	#

	refAssocTypeKey = 0

	if refTypeDict.has_key(refAssocType):
		refAssocTypeKey = refTypeDict[refAssocType]
	else:
		errorFile.write('Invalid Synonym Type (%d) %s\n' % (lineNum, refAssocType))
		refAssocTypeKey = 0

	return(refAssocTypeKey)

def processFile():
	# requires:
	#
	# effects:
	#	Reads input file
	#	Verifies and Processes each line in the input file
	#
	# returns:
	#	nothing
	#

	global refAssocKey

	lineNum = 0
	# For each line in the input file


	for line in inputFile.readlines():

		error = 0
		lineNum = lineNum + 1

		# Split the line into tokens
		tokens = string.split(line[:-1], '\t')

		try:
			accID = tokens[0]
			jnum = tokens[1]
			refAssocType = tokens[2]
#			createdBy = tokens[3]
		except:
			exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

		objectKey = loadlib.verifyObject(accID, mgiTypeKey, None, lineNum, errorFile)
		referenceKey = loadlib.verifyReference(jnum, lineNum, errorFile)
		refAssocTypeKey = verifyRefAssocType(refAssocType, lineNum)
#		createdByKey = loadlib.verifyUser(createdBy, lineNum, errorFile)

		if objectKey == 0 or \
			referenceKey == 0 or \
			refAssocTypeKey == 0 or \
			createdByKey == 0:

			# set error flag to true
			error = 1

		# if errors, continue to next record
		if error:
			continue

		# if no errors, process the marker

		# could move to verifyDuplicate routine

		key = '%s:%s:%s' % (objectKey, referenceKey, refAssocTypeKey)
		if refDict.has_key(key):
		        errorFile.write('Duplicate (%d) %s\n' % (lineNum, line))
			continue

        	refFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
			% (refAssocKey, referenceKey, objectKey, mgiTypeKey, refAssocTypeKey, createdByKey, createdByKey, loaddate, loaddate))

		refAssocKey = refAssocKey + 1

#	end of "for line in inputFile.readlines():"

def bcpFiles():
	# requires:
	#
	# effects:
	#	BCPs the data into the database
	#
	# returns:
	#	nothing
	#

	bcpdelim = "|"

	if DEBUG or not bcpon:
		return

	refFile.close()

	bcp1 = 'cat %s | bcp %s..%s in %s -c -t\"%s" -S%s -U%s' \
		% (passwordFileName, db.get_sqlDatabase(), \
	   	'MGI_Reference_Assoc', refFileName, bcpdelim, db.get_sqlServer(), db.get_sqlUser())

	diagFile.write('%s\n' % bcp1)

	os.system(bcp1)

#
# Main
#

init()
verifyMode()
setPrimaryKeys()
loadDictionaries()
processFile()
bcpFiles()
exit(0)

