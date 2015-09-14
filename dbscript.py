#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

from dbu import MediaItem
from dbu import die
from xmlutils.xml2json import xml2json
import feedparser
import dateutil.parser 

import dbu

import httplib, urllib,base64, json, sys, requests, ssl, urllib3, time,optparse,os, re, pprint
urllib3.disable_warnings()    # use sudo pip install urllib3. This disables the requests ssl warnings

#httpGetRequest = dbUtils.httpGetRequest

#export PYTHONIOENCODING=utf8

# easy-install pip
# sudo pip install requests
# sudo pip install feedparser
# sudo pip install xmlutils

#./manage.py --accountid=37861030001 --keyname=gannett2 --verbose --passitems=10

global _CFG

_CFG = {
		"query": "",
		"configFile": "dbConfig.json",
		"keyname": "",
		"accountId": "",
		"outputdir": "./", 
		"passitems": 100,
		"debug": 0,
		"limit": 0,
		"skiptoid": '',
		"verbose": 0,
		"keyfile": "" 
}


def main(argv):

	loadConfigFile()

	#Command Line arguments and options
	#
	usage = "usage: prog command [options]\n\n" 

	parser = optparse.OptionParser(usage=usage, version="%prog 1.2")

	group1 = optparse.OptionGroup(parser, "Options for Accounts and Media")
	
	group1.add_option("--query", action="store", dest="query", default=_CFG['query'],
	             help="Provide a query to limit the search (ex id:2444)")

	group1.add_option("--accountid", action="store", dest="acctId",default=_CFG['accountId'],
	             help="Process a single account if accountID is provided.")
	
	group1.add_option("--mediaid", action="store", dest="mediaId", default="",
	             help="Display all info on a specific media item")
	
	#######################
	group2 = optparse.OptionGroup(parser, "Update Commands - Require permissions",
                    	"These options update the media items")

	group2.add_option("--delete-masters", action="store_true", dest="deletemaster", default=False,
	             help="Delete the digital masters")

	group2.add_option("--high-as-master", action="store_true", dest="highasmaster", default=False,
	             help="Set the highest MP4 rendition as the new master")

	group2.add_option("--reingest-from-highest", action="store_true", dest="reingesthighest", default=False,
	             help="Re-ingest content using highest rendition.")

	#######################
	group3 = optparse.OptionGroup(parser, "Level of Information to return",
                    	"These options define what information is returned")

	group3.add_option("--countonly", action="store_true", dest="countonly", default=False,
	             help="Just return the number of items in each account")

	group3.add_option("--itemsonly", action="store_true", dest="itemsonly", default=False,
	             help="Process items only (no masters or renditions")

	#######################
	group4 = optparse.OptionGroup(parser, "Default Overrides",
                    	"Override defaults in configuration file")
	
	group4.add_option("--outputdir", action="store", dest="outputdir", default=_CFG['outputdir'],
	             help="Output Directory (Default=./)")
	group4.add_option("--keyname", action="store", dest="keyname", default=_CFG['keyname'],
	             help="Key Name in config file to use")
	group4.add_option("--ingest-profile", action="store", dest="ingestprofile", default=_CFG['ingestprofile'],
	             help="Ingest profile to use for ingestion/re-ingestion")

	group4.add_option("--keyfile", action="store", dest="keyfile", default=_CFG['keyfile'],
	             help="Special case. Ask Dave")
	group4.add_option("--itemcsv", action="store", dest="itemcsv", default="",
	             help="Added for Josef. Path to a file with MediaIDs to process. (just skips items that arent in file)")

	#######################
	group5 = optparse.OptionGroup(parser, "Debugging and screen output")

	group5.add_option("--verbose", action="store_true", dest="verbose", default=_CFG['verbose'],
	             help="Display more info on screen")
	group5.add_option("--debug", action="store", dest="debug", type="int",default=_CFG['debug'],
             help="Enable debugging (1=basic or 2=json 3=print keys)")
	group5.add_option("--passitems", action="store", dest="passitems", default=_CFG['passitems'], type="int",
	             help="Number of items to process per query call (max is 100, default is 50)")

	group5.add_option("--limit", action="store", dest="limit",default=_CFG['limit'],type="int",
	             help="Limit the number of media items to this count")
	
	group5.add_option("--skipto", action="store", dest="skiptoid",default=_CFG['skiptoid'],
	             help="Skip until this ID then start processing)")

	group5.add_option("--testme", action="store_true", dest="testme", default="",
	             help="Test Code.")

	#######################
	group6 = optparse.OptionGroup(parser, "Ingest and Content Item Related ")

	group6.add_option("--ingest-from-json", action="store", dest="ingestjsonfile", default="",
	             help="Ingest from a JSON Feed File  (supports --limit)")

	group6.add_option("--ingest-from-MRSS", action="store", dest="ingestmrssfile", default="",
	             help="Ingest from a MRSS Feed File  (supports --limit)")

	group6.add_option("--get-ingest-profiles", action="store_true", dest="getingestprofiles", default=False,
	             help="Get ingestion profile info for accountid")

	group6.add_option("--copy-item-fields", action="store_true", dest="copyitemfields", default=False,
	             help="Copy the field to an alternate field (specified in the config file)")

	group6.add_option("--delete-media-items", action="store_true", dest="deletemediaitems", default=False,
	             help="Delete Content Items for account (BE CAREFUL). uses --query")

	parser.add_option_group(group1)
	parser.add_option_group(group2)
	parser.add_option_group(group3)
	parser.add_option_group(group4)
	parser.add_option_group(group5)
	parser.add_option_group(group6)
	(options, args) = parser.parse_args()

	# Override config file with command line options
	#
	_CFG['keyname'] = options.keyname
	_CFG['accountId'] = options.acctId
	_CFG['debug'] = options.debug
	_CFG['limit'] = options.limit
	_CFG['skiptoid'] = options.skiptoid
	_CFG['mediaId'] = options.mediaId
	_CFG['query'] = options.query
	_CFG['outputDir'] = options.outputdir
	_CFG['itemsonly'] = options.itemsonly
	_CFG['passitems'] = options.passitems
	_CFG['keyfile'] = options.keyfile
	_CFG['ingestprofile'] = options.ingestprofile
	_CFG['verbose'] = options.verbose
	_CFG['countonly'] = options.countonly
	_CFG['deletemaster'] = options.deletemaster
	_CFG['highasmaster'] = options.highasmaster
	_CFG['reingesthighest'] = options.reingesthighest

	_CFG['itemcsv'] = options.itemcsv

	_CFG['handles'] = { 'real':0, 'shared':0, 'error':0, 'results':0 }

	# _CFG['fieldStr'] = "{id},{name},{reference_id},{masterId},{masterRate},{masterSize},{duration},{created_at},{updated_at},{state},{rendSize},{rendCount},{rendBitrates},{HLSRendSize},{HLSRendCount},{HLSResolutions},{HLSBitrates},{rendLgEncRate},{rendLgRes},{rendSize},{rendLgSize},{rendLgUrl}"
	# _CFG['resultStr'] = "{accountId},{totalCount},{itemcount},{shared},{skipped},{duration},{masterSize},{rendCount},{rendSize},{HLSRendCount},{HLSRendSize},{rendLgSize}"

	_CFG['totalFields'] = { 'masterSize':0, 'masterCount':0, 'duration':0,'rendCount':0, 'rendSize':0, 'rendLgSize':0, 'HLSRendSize':0, 'HLSRendCount':0,
	                        'skipped':0, 'shared':0, 'itemcount':0 , 'totalCount':0}
	_CFG['itemFields'] =  { 'masterSize':0,'duration':0,'masterId':0, 'masterRate':0, 'id':0, 'name':0,'created_at':0,'updated_at':0, 'state':0 }
	_CFG['rendFields'] =  { 'rendCount':0, 'rendSize':0, 'rendLgSize':0, 'rendBitrates':0, 'rendCodecs':0, 'rendLgId':0, 'rendLgEncRate':0, 'rendLgRes':0,'rendLgUrl':0, 
	                        'HLSRendCount':0, 'HLSRendSize':0, 'HLSResolutions':0,'HLSBitrates':0 }
	# AuthToken (gets renewed every 4 minutes)
	_CFG['token'] = ""
	_CFG['tokenLastUpdated'] = 0


	#if len(argv) == 1: parser.print_help(); sys.exit()
	#if not _CFG['mediaId']:	printCfg(_CFG)
	printCfg(_CFG)
	sys.stdout.flush()


	# Change the config options based on passed in flags where necessary
	#
	
	if _CFG['mediaId']:
		_CFG['level'] = { 'items': 1, 'masters': 1, 'renditions': 1, 'hlsrenditions': 1}

	if options.countonly:
		_CFG['level'] = { 'items': 0, 'masters': 0, 'renditions': 0, 'hlsrenditions': 0}

	if options.itemsonly:
		_CFG['level'] = { 'items': 1, 'masters': 0, 'renditions': 0, 'hlsrenditions': 0}

	# need to fetch renditions to set high master.
	if _CFG.get('highasmaster',0) or _CFG.get('reingest',0):
		_CFG['level']['renditions'] = 1;

	if options.testme:
		testme()
		sys.exit()
	elif options.getingestprofiles:
		getIngestionProfiles()
		sys.exit()
	elif options.ingestjsonfile:
		ingestFromJsonFile(options.ingestjsonfile, _CFG['accountId'])
		sys.exit()
	elif options.ingestmrssfile:
		ingestFromMRSSFile(options.ingestmrssfile, _CFG['accountId'])
		sys.exit()
	elif options.copyitemfields:
		copyItemFields()
		sys.exit()
	elif options.deletemediaitems:
		deleteMediaItems()
		sys.exit()
	else:
		processAll()
		sys.exit()

# http://solutions.brightcove.com/bcls/media/ReferenceIDGenerator/reference-id-generator.html
# http://docs.brightcove.com/en/video-cloud/media/solutions/generate-reencode-xml.html


def processAll():

	q=""
	if _CFG['query']: q = "&q="+ _CFG['query']
	if _CFG['mediaId']: q = "&q=id:"+ _CFG['mediaId']

	#
	# Load Account List hash
	#
	if _CFG['keyfile']:
		acctListHash = loadJsonData(_CFG['keyfile'])
		#print "{0} accounts found]: {1}".format(len(accountHash.keys()), accountListHash.keys())
		
		for accountId in sorted(acctListHash.keys()):
			keyname = acctListHash.get(accountId,0)

			if not keyname:
				print "ERROR: No Key found for account: {0}. Skipping\n".format(accountId)
				continue
			
			# Clear and set the token name
			_CFG['token'] = 0
			_CFG['keyname'] = keyname

			# print "Processing account: {0} with key: {1}".format(accountId,keyname)
			processAccount( accountId, q)
	else:
		# Use the accounts in the accountInfoFile and the provided keyname.

		if _CFG.get('accountId',0):
			# Account ID was specifically passed in, use it.
			accountList = [ _CFG['accountId'] ]
		else:
			accountList = getAccountList()	
	
		print "{0} accounts found]: {1}".format(len(accountList), accountList)

		for accountId in accountList:
			processAccount( accountId, q)


def processAccount( accountId, q):

	openFiles(accountId);

	totals = processQuery(accountId, q)
	totals['accountId'] = accountId

	write('results', _CFG['resultStr'].format(**totals))

	closeFiles()


def processQuery(accountId, query):

	results = {}
	
	totals = {}
	totals = _CFG['totalFields'].copy()
	qoffset=_CFG['passitems']

	# Query to get the total number of videos
	queryCount =  getVideoCount(accountId)
	log("Processing Account: {0:15} ({1} items)".format(accountId,  queryCount))
	
	if not _CFG["level"].get('items',0): 
		totals['totalCount'] = queryCount;
		return totals

	limit = _CFG['limit']
	offset=0
	queryList = []

	if _CFG['itemcsv']:
		loadItemCSV(_CFG['itemcsv'])


	while 1:
		
		if offset >= queryCount:
			break
		
		url = "https://cms.api.brightcove.com/v1/accounts/"+accountId+"/videos/?limit="+str(qoffset)+"&offset=" + str(offset) + query
		queryList.append(url)
		offset = offset + qoffset

		# For testing, only process LIMIT entries then stop (rounds up to nearest 100).
		#
	
		if (limit > 0 and offset >= limit):
			log("Passed Query limit hit, breaking")
			break

	offset = 0
	while len(queryList):
		url=queryList.pop(0)

		errorRetries = 0;

		while errorRetries < 5:
			res = httpGetRequest(url)

			rescount = len(res)
			if rescount == 0 or rescount == 99 and len(queryList):
				errorRetries = errorRetries + 1
				log("ERROR!: Query returned too few values. Returned: {0}, expected: {1}".format(rescount, qoffset))
				continue
			break

		if not _CFG['mediaId']: log("**-> Processing {0} items: {1:d} - {2} of {3}".format(len(res), offset,offset+qoffset,queryCount))

		results = processQueryResults(accountId, res)

		# Add the results to the totals
		for key in totals.keys():
			totals[key] = totals[key] + results[key];

		offset = offset + qoffset
	
	if not _CFG['mediaId']:	
		print
		print "Total Video Item Records     : {0} ".format(queryCount)
		print "Total Processed Successfully : {0} ".format(totals['itemcount'])
		print "Shared Items Skipped         : {0} ".format(totals['shared'])
		print "Items Skipped (0 duration)   : {0} ".format(totals['skipped'])
		print "Total Duration               : {0} ".format(totals['duration'])
		print "Total Rendition Count        : {0} ".format(totals['rendCount'])
		print "Total Largest rendition Size : {0} ".format(totals['rendLgSize'])
		print "Total Rendition Size         : {0} ".format(totals['rendSize'])
		print "Total Number Digital Masters : {0} ".format(totals['masterCount'])
		print "Total Digital Master Size    : {0} ".format(totals['masterSize'])
		print "Total Managed Content        : {0} ".format(totals['masterSize'] + totals['rendSize'])
		print

	totals['totalCount'] = queryCount
	if _CFG['mediaId']:
		print totals
	return totals



def processQueryResults(accountId, items):


	debug = _CFG['debug']

	totals = _CFG['totalFields'].copy()

	# Count how many  items we skip and error
	skippedCount=0
	sharedCount=0
	itemCount = 0
	masterCount = 0

	batchRes = {}
	batchRes = _CFG['totalFields'].copy()

	# Loop through all VideoIDs
	for a in items:
			
		shared = 0;
		output = 'real'
		#sys.stderr.write(a['id'])
		sys.stderr.flush()

		if _CFG['skiptoid']:
			if a['id'] == _CFG['skiptoid']:
				_CFG['skiptoid'] = 0
			else:
				skippedCount = skippedCount + 1 
				continue

		if _CFG['itemcsv']:
			# skip if this item is not in the itemCsvMap
			if not _CFG['itemcsvmap'].get(a['id'],0):
				continue

		## XXX THis should all be moved into ProcessItem, but have to deal with the counters...
		#
		if a['duration'] == -1 or not a['duration']:
			if debug: log("*skip: %s has -1 or null duration" % (a['id']))
			skippedCount = skippedCount + 1
			shared = 2  # error
			res = {}

			res = _CFG['itemFields'].copy()
			res.update(_CFG['totalFields'])

			res['id'] = a['id']
			res['name'] = re.sub(',','.',a['name']).encode('ascii','ignore')
			res['reference_id'] = re.sub(',','.',getWithDefault(a,'reference_id',"")).encode('ascii','ignore')
			res['created_at'] = a['created_at']
			res['updated_at'] = a['updated_at']
			res['state'] = a['state']
			
			write('error',"{id},{name},{created_at},{updated_at},{state}".format(**res))
			if _CFG['verbose']:
				log("{id},{name},{created_at},{updated_at},{state}".format(**res))
			
			continue

		# See if this is a shared asset. If yes, skip. sharing will be a key, but it will be None uless the asset is shared.
		#
		# If there is no sharing object, then this a single.
		#  source_id is set if this is a shared copy, do not count
		#   
		# FROM THE DOCS
		# by_external_acct	boolean	yes	false	whether the video was shared from another account
		# by_id				string	yes	null	id of the account that shared the video - note that this field is populated only for the shared copy, not for the original video
		# source_id			string	yes	null	id of the video in its original account - note that this field is populated only for the shared copy, not for the original video
		# to_external_acct	boolean	yes	false	whether the video is shared to another account
		# by_reference		boolean	yes	false	whether the video is shared by reference[8-1]

		if 'sharing' in a.keys() and a['sharing']:

			sh = a['sharing']
			if sh.get('source_id',0):
				sharedCount = sharedCount + 1
				output = 'shared'
				shared = 1;
		
		if output == 'real':
			itemCount = itemCount + 1
			if a.get('digital_master_id',0):
				masterCount = masterCount + 1

		itemRes = processVideoItem(accountId, a);

		write(output, _CFG['fieldStr'].format(**itemRes))
		if _CFG['verbose']:
			log(_CFG['fieldStr'].format(**itemRes))

		for key in totals.keys():
			if key in itemRes:
				batchRes[key] = batchRes[key] + itemRes[key]

		if _CFG.get('reingesthighest',0):
			ingestVideo(accountId, itemRes)
			

	# Add the counters for the batch		
	batchRes['skipped'] = skippedCount
	batchRes['shared'] = sharedCount
	batchRes['itemcount'] = itemCount
	batchRes['masterCount'] = masterCount

	#print batchRes
	return batchRes

def getWithDefault(d, key, deflt):

	res = d.get(key,deflt)
	if (not res):
		return ""
	else:
		return res

def processVideoItem(accountId, a):

	res = {}
	itemResults = {}
	itemResults = _CFG['itemFields'].copy()

	itemResults['id'] = a['id']
	itemResults['name'] = re.sub(',','.',a['name']).encode('ascii','ignore')

	ref = getWithDefault(a, 'reference_id',"")
	itemResults['reference_id'] = re.sub(',','.',ref).encode('ascii','ignore')

	itemResults['created_at'] = a.get('created_at',"")
	itemResults['updated_at'] = a.get('updated_at',"")
	itemResults['state'] = a.get('state',"")

	if _CFG['mediaId']:	
		log("Video Item: {0}".format(a['id']))
		log(json.dumps(a,indent=4, sort_keys=True))

	try:
	
		# Get the DigitalMaster if one is defined.
		masterRate = 0
		masterSize = 0
		hasMaster = 0
		if a['digital_master_id']:
			master = getDigitalMaster(accountId, a['id'])
		
			if 'encoding_rate' in master.keys() and master['encoding_rate']:
				masterRate = master['encoding_rate']/1000	
			if 'size' in master:
				masterSize = master['size']
				
			itemResults['masterId'] = master['id']
			hasMaster = 1

			if _CFG['deletemaster']:
				deleteDigitalMaster(accountId, a['id'])
				log("{0}: DIGITAL MASTER Deleted (master id={1})".format(a['id'], master['id']))
				hasMaster = 0


		itemResults['masterSize'] = masterSize;
		itemResults['masterRate'] = masterRate;

		rendResults = getVideoRenditions(accountId, a['id'], a['duration']/1000, hasMaster)

		itemResults.update(rendResults)

		itemResults['duration'] = a['duration']/1000

	except KeyboardInterrupt:
		sys.exit()
	# except:
	# 	print("CAUGHT EXCEPTION", sys.exc_info()[0])
	# 	itemResults.update( _CFG['rendFields'] )
	# 	return itemResults;

	return itemResults


def getDigitalMaster(accountID, videoID):

	if not _CFG['level'].get('masters',0):
		return { 'id':0};

	url = "https://cms.api.brightcove.com/v1/accounts/"+accountID+"/videos/" + videoID + "/digital_master"
	res = httpGetRequest(url)

	if _CFG['mediaId']:	
		log("DIGITAL MASTER:")
		log(json.dumps(res,indent=4, sort_keys=True))

	return res

def isGannettAccount(accountId):
	file = open('account-list.txt')
	for line in file:
		acct = line.rstrip('\n')

		if acct == accountId: 
			return True
	else:
		return False


def getVideoRenditions(accountID,mediaID, duration, hasMaster):
	
	if not _CFG['level'].get('renditions',0):
		return _CFG['rendFields'].copy()

	debug = _CFG['debug']

	url = "https://cms.api.brightcove.com/v1/accounts/"+accountID+"/videos/" + mediaID + "/sources/";

	res = httpGetRequest(url)

	progressive = {}
	adaptive = {}
	rendResults = _CFG['rendFields'].copy()

	if _CFG['mediaId']: log("RENDITIONS: found {0} renditions".format(len(res)))

	# Loop through each rendition and combine by asset ID or container. 
	#   There are multiple items with the same asset id (Src and stream)
	#   There are multiple M2TS items (one for regular, one for secure)
	#   Create a hash of the asset_id/Container and then the fields.
	for a in res:
		
		if _CFG['mediaId']:	log(json.dumps(a,indent=8, sort_keys=True))

		rend = {}
		for b in a.keys():
			rend[b] = a[b]

		if "asset_id" in a:
			progressive[a["asset_id"]] = rend
		else:
			# No Asset ID. 
			# IF Container is M2TS, then this is HLS. There could be 1 or two of these. (regular and secure=true)
			# for now, we will keep the one that does not have secure=true in the URL
			container = a.get('container',"") 
			src = a.get('src',"") 

			if container == 'M2TS':
				# See if this is the scecure URL
				if not re.search("secure=true", src):
					# This is not the secure.
					adaptive = getHLSRenditions(src, duration)
					rendResults.update(adaptive)
			else:
				log("Unhandled Container type: {0}, skipping".format(container))

	#loop through renditions to find the largest and total size for the progressive renditions
	
	codecHash = {}
	bitrateHash = {}
	biggestSize=0
	totalSize = 0 
	biggestURL = ""

	for a in progressive.keys():

		b = progressive[a]

		if 'codec' in b.keys(): codecHash[b['codec']] = 1

		encodingRate = 0
		if 'encoding_rate' in b.keys() and b['encoding_rate']: 
			encodingRate = b['encoding_rate']/1000
		
		bitrateHash[encodingRate] = 1

		if 'size' in b.keys():
			totalSize = totalSize + b["size"]

			if b["size"] > biggestSize:
				biggestSize = b["size"]

				biggestURL = b.get('src',"")
				rendResults["rendLgId"] = b.get('asset_id',0)

				#Double check this, not sure.
				rendResults["rendLgRes"] = "(" + str(b['width']) + "x" + str(b['height']) +")"
				rendResults['rendLgEncRate'] = str(encodingRate)


	# set largest rendition as Master.
	if _CFG['highasmaster'] and rendResults["rendLgId"] and hasMaster == 0:
		setRenditionAsMaster(accountID, mediaID, rendResults["rendLgId"])

	rendResults["rendCount"] = len(progressive.keys())
	rendResults["rendSize"] = totalSize
	rendResults["rendLgSize"] = biggestSize
	rendResults["rendLgUrl"] = biggestURL
	rendResults['rendBitrates'] = getKeyListStr(bitrateHash,"|")
	rendResults['rendCodecs'] = getKeyListStr(codecHash,"|")
	
	return rendResults

def getHLSRenditions(url, duration):

	if not _CFG['level'].get('hlsrenditions',0):
		return {}

	results = {}
	totalSize = 0
	count = 0
	resolution = [];
	bitrates = [];

	try:
		res = httpGetContents(url)
	except ValueError as err:
		log("ERROR: GetHLSRenditions Error {0}. Skipping HLS Get. {1}".format(err,url))
		return {}

	for line in res.split():
		search = re.search( r'BANDWIDTH=(\d+).*RESOLUTION=(\d+x\d+)',line, re.M|re.I)
		if search:
			br = int(search.group(1))
			bitrates.append(br/1000)
			resolution.append(search.group(2))
			size =  br * 60/1024/8/60 * duration
			totalSize = totalSize + size
			count = count + 1
		
		results['HLSRendCount'] = count
		results['HLSRendSize'] = totalSize
		results['HLSResolutions'] = listToStr(resolution,'|')
		results['HLSBitrates'] = listToStr(bitrates,'|')

	return results


def deleteDigitalMaster(accountId, mediaId):

	url = "https://cms.api.brightcove.com/v1/accounts/"+accountId+"/videos/" + mediaId + "/digital_master"

	try:
		res = httpDeleteRequest(url)
	except ValueError as err:
		log("ERROR: DeleteMaster failed. Error {0}. Skipping. {1}".format(err,url))
		


def openFiles(accountId):
	handles = _CFG['handles']
	outputDir = _CFG['outputDir'].rstrip('/')

	if not os.path.exists(outputDir):
		os.makedirs(outputDir)

	#print "{0}/{1}.csv".format(outputDir,accountId)

	try:
		handles['real']    = open( "{0}/{1}.csv".format(outputDir, accountId),"w" )
		handles['shared']  = open( "{0}/{1}-shared.csv".format(outputDir,accountId),"w" )
		handles['error']   = open( "{0}/{1}-error.csv".format(outputDir,accountId),"w" )

		write('real',_CFG['fieldStr'])
		write('shared',_CFG['fieldStr'])

		resultFile = "{0}/results.csv".format(outputDir,accountId)
		if os.path.isfile(resultFile):
			handles['results'] = open( resultFile,"a" )
		else:
			handles['results'] = open( resultFile ,"w" )
			write('results', _CFG['resultStr'])
			print _CFG['resultStr']

	except:
		print "Unexpected error:", sys.exc_info()[0]
		raise

def closeFiles(): 

	_CFG['handles']['real'].close()
	_CFG['handles']['shared'].close()
	_CFG['handles']['error'].close()
	_CFG['handles']['results'].close()


def write(*args):

	# Pop the 1st argument off, this indicates which file to write to.
	ar = map(str,args)
	desc = ar.pop(0)

	handle = _CFG['handles'][desc]
		
	handle.write(' '.join(ar) + '\n')
	handle.flush()

	#sys.stdout.write('.')
	#sys.stdout.write(' '.join(map(str,args)) + '\n')
	sys.stdout.flush()

def httpGetRequest(url,returnOn401=False):

    authToken = getAuthToken()
    if not authToken:
        print "Error retrieving auth Token"
        sys.exit()

    headersMap = {
        "Authorization": "Bearer " + authToken,
        "Content-Type": "application/json"
    }

    if _CFG['debug']: log("HTTP GET: " + url)

    #
    # Try 3 times if it fails to eliminate timeout cases
    failed = 0
    f = 0
    while failed < 10:
        r = requests.get(url,headers=headersMap)

        if r.status_code != 200: 
            if r.status_code == 400 or r.status_code == 401:
                # Don't retry on permission denied.
                if returnOn401:
                    return {"error": r.status_code}
                else:
                    break

            print "http request failed: " + url + ", retrying: " + str(r)
            print "Error: {0}".format(r)
            f = f + 1
            failed += 1
            if (failed > 5 ): time.sleep(1) 

            next
        else:
            break
    
    if r.status_code != 200:
        print " Retries Failed. HTTP Request {0} failed: {1}".format(url,r)
        error("http get request failed",r) 
    
    return r.json()


def httpGetContents(url):

	headersMap = {}
	if _CFG['debug']: log("HTTP GET: " + url)
		
	r = requests.get(url,headers=headersMap)

	if r.status_code != 200:
		raise ValueError(r.status_code)
	
	return r.text
	



def httpDeleteRequest(url):

	authToken = getAuthToken()
	if not authToken:
		print "Error retrieving auth Token"

	if _CFG['debug']: log("HTTP GET: " + url)

	headersMap = {
		"Authorization": "Bearer " + authToken,
		"Content-Type": "application/json"
	}

	r = requests.delete(url,headers=headersMap)		
	if r.status_code != 204:
		log("Delete failed: {0}".format(url))
		raise ValueError(r.status_code)
	
	return

def httpPostRequest(url, data):

	authToken = getAuthToken()
	if not authToken:
		print "Error retrieving auth Token"

	if _CFG['debug']: log("HTTP POST: " + url)

	headersMap = {
		"Authorization": "Bearer " + authToken,
		"Content-Type": "application/json"
	}

	r = requests.post(url,data=json.dumps(data), headers=headersMap)

	if r.status_code != 200 and r.status_code != 201:
		log("Post failed: [{0}]: {1} for {2}".format(r, r.json(), url))
		raise ValueError(r.status_code)
	return r.json()



def httpPatchRequest(url, data):

	authToken = getAuthToken()
	if not authToken:
		print "Error retrieving auth Token"

	if _CFG['debug']: log("HTTP PATCH: " + url)

	headersMap = {
		"Authorization": "Bearer " + authToken,
		"Content-Type": "application/json"
	}

	r = requests.patch(url,data=json.dumps(data), headers=headersMap)

	if r.status_code != 200 and r.status_code != 201:
		log("Patch failed: [{0}] for {1}".format(r, url))
		raise ValueError(r.status_code)
	return

def setRenditionAsMaster(accountID, mediaId, renditionId):


	log("{0}: Setting High Rendition ({1}) as master".format(mediaId, renditionId))
	
	authToken = getAuthToken()
	if not authToken:
		print "Error retrieving auth Token"

	url = "https://cms.api.brightcove.com/v1/accounts/"+accountID+"/videos/" + mediaId

	headersMap = {
		"Authorization": "Bearer " + authToken,
		"Content-Type": "application/json"
	}

	dataMap = { "digital_master_id": renditionId }

	r = requests.patch(url,data=json.dumps(dataMap),headers=headersMap)	
	if r.status_code == 200 or r.status_code == 201:
		return
	else:
		log("{0}: Error [{1}]. Failed to assign higest rendition {2} as master".format(mediaId, r.status_code, renditionId))
		return




def getAuthToken():

	# token is arleady defined, see if it is still valid
	if (_CFG['token']):
		now = time.time()

		# Renew after 4 minutes (tokens expire in 5)
		if (now - _CFG['tokenLastUpdated'] <= 240):
			# Token still good.
			return _CFG['token']

	# Renew the token	
	accountInfo = _CFG['accountInfo']
	keyInfo = accountInfo[_CFG['keyname']]
	clientId = keyInfo['apiClient']
	clientSecret = keyInfo['apiSecret']

	url="https://oauth.brightcove.com/v3/access_token"
	authString = base64.encodestring('%s:%s' % (clientId, clientSecret)).replace('\n', '')
	
	headersMap = {
		"Content-Type": "application/x-www-form-urlencoded",
		"Authorization": "Basic " + authString
	}	
	
	paramsMap = {
		"grant_type": "client_credentials"
	}
	r = requests.post(url, params=paramsMap,headers=headersMap)

	if r.status_code == 200 or r.status_code == 201:
		res = r.json()
		_CFG['token'] = res['access_token']
		_CFG['tokenLastUpdated'] = time.time()
		return res['access_token']
	else:
		log("Error retrieving auth Token: {0}".format(r))
		return 0
		#msg="Error retrieving auth Token: "+ tokenName
		#error(msg,r)

def getAccountList():

	accountInfo = _CFG['accountInfo']
	keyInfo = accountInfo[_CFG['keyname']]
	accountList = sorted(keyInfo['accounts'])
	#accountList = keyInfo['accounts']

	return accountList


def getVideoCount(accountId ):
	
	q=""
	if _CFG['query']: q = "?q="+ _CFG['query']
	if _CFG['mediaId']: q = "?q=id:"+ _CFG['mediaId']
	
	results = []
	url = "https://cms.api.brightcove.com/v1/accounts/"+accountId+"/counts/videos/" + q
	res = httpGetRequest(url)

	return res['count']


def getVideoItems(accountId, query):

	
	# Query to get the item Count, good to know.
	#
	queryCount =  getVideoCount(accountId)
	log("* Query Count: %d\n" % (queryCount))
	sys.stdout.flush()

	limit = _CFG['limit']

	results = []
	offset=0

	while 1:
		url = "https://cms.api.brightcove.com/v1/accounts/"+accountId+"/videos/?limit=100&offset=" + str(offset) + query
		res = httpGetRequest(url)
		# sys.stderr.write(".")
		#sys.stderr.flush()
		results = results + res
		print "* results is now: " + str(len(results))
		sys.stdout.flush()
				
		# If we get back less than 100, then we are done
		if len(res) < 100:
			break
		offset = offset + 100

		if (limit > 0 and offset >= limit):
			log("Query limit hit, breaking")
			break
		
	sys.stderr.write("\n\n")

	return results


def loadConfigFile():
	
	acctFile = _CFG.get('configFile', 'cfg.json')

	# Put some error handing in here...
	try:
		data = open(acctFile) 
	except IOError as e:
		log("Error: Cannot open configuration file {0}: {1}".format(acctFile, e))
		sys.exit()

	try:
		res = json.load(data)
	except ValueError as e:
		log("Error: Issue loaded with JSON file {0}: {1}".format(acctFile, e))
		sys.exit()
	_CFG.update(res)


def loadItemCSV(itemcsv):

	file = open(itemcsv,'r')
	log("Opening ItemCSV File: {0}".format(itemcsv))
	itemCsvMap = {}

	for line in file:
		search = re.search( r'(\d+),',line, re.M|re.I)
		if search:
			itemCsvMap[search.group(1)] = 1
	
	_CFG['itemcsvmap'] = itemCsvMap


def setCredentials():

	print

	# 	curl \
	# 	--include \
	# 	--header 'Authorization: BC_TOKEN  
	# AEnTxTioyv4YvceKOUuqa_KsCZ2Yl7K0jhpJ3TWBNRrarOObSWnHrVQ6rikuS1YivXlIh8RSwjf877RGxhJ0aKVLoTl3EXo_VLPVZP2dpzg6KrbmkXlpixw' \
	# 	--data 'name=deleteMasterProfile&maximum_scope=[
	# 	{"identity": {
	# 	"type": "video-cloud-account", "account-id": 4113679519001
	# 	},
	# 	"operations": [
	# 	"video-cloud/asset/delete", 
	# 	"video-cloud/video/all",
	# 	"video-cloud/ingest-profiles/profile/read",
	# 	"video-cloud/ingest-profiles/profile/write",
	# 	"video-cloud/ingest-profiles/account/read",
	# 	"video-cloud/ingest-profiles/account/write"
	# 	]
	# 	}
	# 	]' \
	# 	https://oauth.brightcove.com/v3/client_credentials




def log(*args):
    sys.stderr.write(' '.join(map(str,args)) + '\n')
    sys.stderr.flush()

def printCfg(cfg):
	for a in cfg.keys():
		if a not in ['apiSecret', 'accountInfo','token','resultStr','handles','tokenLastUpdated','itemFields','rendFields','totalFields','fieldStr']:
			print "%18s : %s" % (a,cfg[a])

def loadJsonData(file):
	# Load from .json file

	# Put some error handing in here...
	json_data = open(file)
	res = json.load(json_data)
	return res

def getKeyListStr(dict, delim):
	res = ""
	d = dict.keys()
	d.sort()
	for a in d:
		res = res + str(a) + delim
	return res

def listToStr(l, delim):
	res = ""
	d = sorted(l)
	for a in d:
		res = res + str(a) + delim
	return res

def error(msg,r):

	print r.json()
	print "\t*%s[%d] : %s" % (msg,r.status_code,r.reason)
	sys.exit()


def ingestVideo(accountId, itemRes):
  
  	url="https://ingest.api.brightcove.com/v1/accounts/{0}/videos/{1}/ingest-requests".format(accountId, itemRes['id']);

	rendLgUrl = itemRes.get('rendLgUrl', "")
	profileName = _CFG.get('ingestprofile',"")
	if not rendLgUrl: return

	ingest_data = {
		'profile' : profileName,
		'master': {'url': rendLgUrl},
		'capture-images': False
	}

	try:
		r = httpPostRequest(url, ingest_data)
		if _CFG.get('verbose',0):
			log("re-ingesting {0} with profile {1}".format(itemRes.get('id',""), profileName))
	except ValueError as err:
		log("re-ingest failed: {0} ".format(err))
		return


def deleteMediaItems():
	q1 = MediaItem(_CFG, _CFG['accountId'])
	q1.Query(_CFG['query'])
	log("Query returned [{0}] items.".format(q1.QueryCount()))
	
	item = q1.Next()
	while item:
		print "Deleting: [{0}]: id: {1} refid: {2}".format(q1.returnedCount,item['id'], item['reference_id'])
		
		r = q1.Delete(item['id'])
		if r:
			log("Delete failed: [{0}]".format(r))

		item = q1.Next()
	
	

def getIngestionProfiles():

	accountId = _CFG.get('accountid',"")

	if not accountId:
		accountList = getAccountList()
		accountId = getAccountList()[0]
		
	log("\nRetrieving Ingest Profile for: {0}".format(accountId))


	url = "https://ingestion.api.brightcove.com/v1/accounts/{account_id}/profiles".format(account_id=accountId)
	res = httpGetRequest(url)

	for a in res:
		print "{0:15s}: {1}".format(a['id'],a['name'])

	if _CFG.get('verbose',0):
		log(json.dumps(res,indent=4, sort_keys=True))


def copyItemFields():
	q1 = MediaItem(_CFG, _CFG['accountId'])
	q1.Query(_CFG['query'])

	item = q1.Next()
	while item:
		print "[{0}]: id: {1} refid: {2} long: {3}".format(q1.returnedCount,item['id'], item['reference_id'],item['long_description'])
		
		if item:
			data = {}

			data['long_description'] = item.get('reference_id',"")
			if not item.get('long_description', None):
				q1.Update( item['id'], data)
				print "Updated."

		item = q1.Next()



def ingestFromJsonFile( jsonFile, accountId):

	if not accountId:
		log("\nError: This command requires accountId be set.")
		sys.exit()

	try:
		data = open(jsonFile) 
	except IOError as e:
		 log("\nError: Cound not open file: {0}: error {1}".format(jsonFile, e))
		 sys.exit()
	
	try:
		jsonData = json.load(data)
	except ValueError as e:
		log("\nError: Invalid Json File {0}: {1}".format(jsonFile, e))
		sys.exit()

  	count = 0
  	limit = _CFG.get('limit',0)

  	mi = MediaItem(_CFG, _CFG['accountId'])

	items = jsonData.get('items',[])
	for item in items:

		if limit > 0 and count >= limit:
			log("Defined limit of {0} reached.".format(limit))
			break

		count = count + 1

		if not item.get('title',""):
			log("title required to process item, skipping: {0}".format(item))
			continue

		if not item.get('video_url',""):
			log("video_url required to process item, skipping: {0}".format(item))
			continue

		itemData = {}
		customFields = {}
	
		customFields['rating'] = item.get('rating',"")
		customFields['publish_date'] = item.get('publish_date',"")
		customFields['original_publish_date'] = item.get('original_publish_date',"")
		customFields['website'] = "CollegeHumor"

		
		itemData['name'] = item.get('title',"")[:256]
		itemData['description'] = item.get('description',"")[:250]

		if item.get('original_publish_date',""):
			schedule = {}
			date = dateutil.parser.parse(item.get('original_publish_date',""))
			schedule['starts_at'] = date.isoformat()
			itemData['schedule'] = schedule;

		# For electus guid is /post/xxx or /video/xxx. Just use xxx as the refid
		search = re.search(r'/.*/(\d+)',item.get('guid',""),re.M|re.I)
		if search:
			itemData['reference_id'] = search.group(1)

		if item.get('tags',""):
			itemData['tags'] = item.get('tags',[]).split(',')
		
		itemData['custom_fields'] = customFields;

		res = mi.Create(itemData)
	
		if not res:
	 		# Error creating object, skip the ingest. (if goal is to re-ingest, don't skip)
			print "ERROR creating media item {0}, skipping.".format(itemData['reference_id'])
			continue

		videoId = res.get('id',"")
		log("[{0}] created.".format(videoId))
		
		ingestData = {}
		master = {}
		thumb = {}
		
		master['url'] = item.get('video_url',"")
		thumb['url'] = item.get('thumbnail_url',"")
		ingestData['master'] = master
		ingestData['thumbnail'] = thumb
		ingestData['poster'] = thumb

		if _CFG.get('ingestprofile',""): 
			ingestData['profile'] = _CFG['ingestprofile']
		
		r = mi.Ingest(videoId, ingestData)
		if not r:
			log("Ingest Failed")
			continue

		log("[{0}] Ingested. JobId={1}".format(videoId,r.get('id',"")))


	
def ingestFromMRSSFile(mrssFile, accountId):

	count = 0
  	limit = _CFG.get('limit',0)

  	mi = MediaItem(_CFG, _CFG['accountId'])
	
	feed = feedparser.parse( mrssFile )
	items =  feed['items']

	log("Processing {0} items in MRSS feed".format(len(items)))
	for i in items:

		if limit > 0 and count >= limit:
			log("Defined limit of {0} reached.".format(limit))
			break

		count = count + 1

		# for k in i.keys():
		# 	print "{0}: {1}\n".format(k,i.get(k,""))

		itemData = {}
		itemData['name'] = i.get('title',"")[:256]
		itemData['description'] = i.get('summary'"")[:250]
		itemData['reference_id'] = i.get('id',"")
		#itemData['tags'] = i.get('media_tags',"").split(',')
		#itemData['custom_fields'] =

		res = mi.Create(itemData)
	
		if not res:
	 		# Error creating object, skip the ingest. (if goal is to re-ingest, don't skip)
			print "ERROR creating media item {0}, skipping {1}.".format(itemData['reference_id'], itemData)
			continue

		videoId = res.get('id',"")
		log("[{0}] created.".format(videoId))

		ingestData = {}
		master = {}
		thumb = {}
		
		master['url'] = i['media_content'][0].get('url',"")
		thumb['url'] = i['media_thumbnail'][0].get('url',"")

		ingestData['master'] = master
		ingestData['thumbnail'] = thumb
		ingestData['poster'] = thumb

		if _CFG.get('ingestprofile',""): 
			ingestData['profile'] = _CFG['ingestprofile']
		
		r = mi.Ingest(videoId, ingestData)
		if not r:
			log("Ingest Failed")
			continue

		log("[{0}] Ingested. JobId={1}".format(videoId,r.get('id',"")))



def copyItemFields():
	q1 = MediaItem(_CFG, _CFG['accountId'])
	q1.Query(_CFG['query'])

	item = q1.Next()
	while item:
		print "[{0}]: id: {1} refid: {2} long: {3}".format(q1.returnedCount,item['id'], item['reference_id'],item['long_description'])
		
		item = q1.Next()
		customFields = item.get('custom_fields',{})
		customFields['website'] = 'CollegeHumor'

		date = dateutil.parser.parse(customFields.get('original_publish_date',""))

		updateData = {}
		schedule = {}
		schedule['starts_at'] = date.isoformat()
		updateData['schedule'] = schedule;
		updateData['custom_fields'] = customFields
		
		q1.Update( item['id'], updateData)
		print "{0}: Updated.".format(item['id'])

		item = q1.Next()

def createApiCredentials():
	name = "Dave"
	desc = "Dave All Permissions"
	permissionList = [ 'video-cloud/asset/delete',
	                   'video-cloud/ingest-profiles/profile/read',
					   'video-cloud/ingest-profiles/profile/write',
					   'video-cloud/ingest-profiles/account/read',
					   'video-cloud/ingest-profiles/account/write'
  					 ]  					 

  	accountList = [4113679519001, 4165107200001]
  	bcToken = "AEnTxTgi_WYFe0696F_R5-T48GzX27wJWig2TCGA6jn4LYtXcbqbF4Gypmf9QigtEp06uUCrfpIL_ExVJQLHC1VrIRL3_3GLiv8gOXXAiA4UR3dEssuSNgI"

  	r = dbu.createOathCredentials(name,desc,permissionList,accountList,bcToken)
  	print r



def testme():



	# text = 'Thu, 16 Dec 2010 12:14:05 +0000'
	# date = (dateutil.parser.parse(text))
	# print(date.isoformat())
	# # 2010-12-16T12:14:05+00:00
	# sys.exit()




	q1 = MediaItem(_CFG, _CFG['accountId'])
	q1.Query(_CFG['query'])
	log("Query returned [{0}] items.".format(q1.QueryCount()))

	item = q1.Next()
	while item:
		print "item: [{0}]: id: {1} refid: {2}".format(q1.returnedCount,item['id'], item['reference_id'])
		
		# r = q1.Delete(item['id'])
		# if r:
		# 	log("Delete failed: [{0}]".format(r))

		item = q1.Next()
	
	

if __name__ == "__main__":
    main(sys.argv)
sys.exit()





