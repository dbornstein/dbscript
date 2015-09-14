#!/usr/bin/python

import httplib, urllib,base64, json, sys, requests, ssl

name = "APIKeyName"
description = "Description of Key"

bcToken = "AEnTxTgi_WYFe0696F_R5-T48GzX27wJWig2TCGA6jn4LYtXcbqbF4Gypmf9QigtEp06uUCrfpIL_ExVJQLHC1VrIRL3_3GLiv8gOXXAiA4UR3dEssuSNgI"

permissionList = [ 'video-cloud/asset/delete',
                   'video-cloud/ingest-profiles/profile/read',
                   'video-cloud/ingest-profiles/profile/write',
                   'video-cloud/ingest-profiles/account/read',
                   'video-cloud/ingest-profiles/account/write'
                 ]    

# accountIds must be numbers, not strings
accountList = [4113679519001, 4165107200001]

#########################################

data = {}
data['type'] = 'credential'
data['name'] = name
data['description'] = description
data['maximum_scope'] = []

for acctId in accountList:

    identity = {}
    identity['type'] = "video-cloud-account"
    identity['account-id'] = acctId
    item = {}
    item['identity'] = identity
    item['operations'] = permissionList

    data['maximum_scope'].insert(0,item)


headersMap = {
    "Content-Type": "application/json",
    "Authorization": " BC_TOKEN " + bcToken
}

#print(json.dumps(data,indent=4, sort_keys=False))

url = 'https://oauth.brightcove.com/v3/client_credentials'
r = requests.post(url,data=json.dumps(data), headers=headersMap)

# Something did not work
if (r.status_code != 201):
    print "status code: {0}".format(r.status_code)
    print r.json()
    sys.exit()

results = r.json()
acct = {}
acct['apiClient'] = results.get('client_id','')
acct['apiSecret'] = results.get('client_secret','')
acct['comment'] = results.get('description','')
acct['accounts'] = accountList

a = { 'name': acct }
print(json.dumps(a,indent=4, sort_keys=False))

# print "name         : {0}".format(results.get('name',''))
# print "client id    : {0}".format(results.get('client_id',''))
# print "client secret: {0}".format(results.get('client_secret',''))





