import httplib, urllib, base64, json, sys, requests, ssl, urllib3,time

#urllib3.disable_warnings()    # use sudo pip install urllib3. This disables the requests ssl warnings




#
# Queries Media items. 
# 
# Usage: 
# mi = Media(_CFG, accountId)
# 
# Methods
# mi.Query( query="")  // Get all media items
# mi.Next()
# mi.QueryCount()
# mi.Update
# mi.Delete
# mi.Create
# mi.Ingest


class MediaItem:

    def __init__(self, cfg, accountId):

        if not accountId:
            raise ValueError("Error: Missing Account Id")

        self.cfg = cfg    
        self.accountId = accountId
        self.isQuery = 0

    def Create(self, data):

        createUrl = "https://cms.api.brightcove.com/v1/accounts/{0}/videos".format(self.accountId)
        try:
            r = self.httpPostRequest(createUrl, data )
        except ValueError as e:
            #if str(e) == "409":
                # Duplicate reference id, skipping.
             
            return None

        return r



    def Ingest(self, videoId, ingestData):

        url="https://ingest.api.brightcove.com/v1/accounts/{0}/videos/{1}/ingest-requests".format(self.accountId, videoId)
        try:
            r =  self.httpPostRequest(url, ingestData);
        except ValueError as e:
            log("Error[{0}]: Ingest Failed.".format(str(e)))
            return None
        
        return r


    def Delete(self, videoId):

        url = "https://cms.api.brightcove.com/v1/accounts/{0}/videos/{1}".format(self.accountId, videoId)

        authToken = self.getAuthToken()

        headersMap = {
            "Authorization": "Bearer " + authToken,
            "Content-Type": "application/json"
        }

        r = requests.delete(url,headers=headersMap) 

        if r.status_code == 204:
            return None
        else:
            print "Error: Delete Failed: {0}".format(r)
            return r.status_code


    def Update(self, itemId, data):

        url = "https://cms.api.brightcove.com/v1/accounts/{0}/videos/{1}".format(self.accountId, itemId)

        _CFG = self.cfg
        authToken = self.getAuthToken()

        headersMap = {
            "Authorization": "Bearer " + authToken,
            "Content-Type": "application/json"
        }

        retry = 0
        while retry < 3:
            r = requests.patch(url,data=json.dumps(data),headers=headersMap) 

            if r.status_code == 504:
                log("Warning: Gateway Timeout on Update. Retrying")
                retry += 1
                time.sleep(1)
                continue

            if r.status_code == 200 or r.status_code == 201:
                 return
            else:
                print "Error: Update Failed: {0}".format(r)
                raise ValueError(r.status_code)


    def Query(self, query=""):

        self.isQuery = 1
        self.query = ""
        if query: self.query = "q={0}".format(query)

        self.limit = self.cfg['limit']
        self.qcount = None;
        
        # Variables to track in-process Query. 
        self.queryList = self.buildQueryList()
        self.results = []
        self.returnedCount = 0

    def Next(self):

        if len(self.results):

            if self.limit > 0 and self.returnedCount >= self.limit:
                log("Limit override hit, breaking")
                return None

            self.returnedCount +=1
            return self.results.pop(0)

        if len(self.queryList):
            url = self.queryList.pop(0)

            retry = 0
            while retry < 3:
                self.results = self.httpGetRequest(url)

                if len(self.queryList):
                    # There are still more queries to go. We should have 'offset' items
                    # This check is due to weird bug where we get 99 or 0 back sometimes

                    rescount = len(self.results)
                    if rescount == 100:
                        # we are good
                        break
                    else:
                        print("WARNING: Query returned too few values. Returned: {0}, expected: {1}.".format(rescount, self.cfg['passitems']))
                        retry += 1
                else:
                    # There are no more queries, less than 100 is ok.
                    break

            # We now have results, call next to get it.
            return self.Next()

        # We are done.
        return None   

    def QueryCount(self):
        
        if self.qcount:
            return self.qcount;

        url = "https://cms.api.brightcove.com/v1/accounts/{0}/counts/videos/?{1}".format(self.accountId,self.query)
        try:
            res = self.httpGetRequest(url)
        except ValueError as e:
            die("Error: Could not access account[{0}], exiting".format(self.accountId))

        self.qcount = res.get('count',-1)
        return self.qcount



    def buildQueryList(self):

        _CFG = self.cfg

        queryCount = self.QueryCount()
        passitems = _CFG['passitems']
        limit = _CFG['limit']

        queryList = []

        offset=0
        while 1:

            if offset >= queryCount:
                break

            url = "https://cms.api.brightcove.com/v1/accounts/{0}/videos/?limit={1}&offset={2}&{3}".format(self.accountId, passitems, offset, self.query)
            queryList.append(url)
            offset = offset + passitems

        return queryList


    


    

    def getAuthToken(self):

        _CFG = self.cfg

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


    def httpGetRequest(self,url):

        _CFG = self.cfg

        authToken = self.getAuthToken()
        if not authToken:
            print "Error retrieving auth Token"
            sys.exit()

        headersMap = {
            "Authorization": "Bearer " + authToken,
            "Content-Type": "application/json"
        }

        if _CFG['debug']: print("HTTP GET: " + url)

        #
        # Try 3 times if it fails to eliminate timeout cases
        failed = 0
        f = 0
        while failed < 10:

            r = requests.get(url,headers=headersMap)
            
            if r.status_code != 200: 
                if r.status_code == 400 or r.status_code == 401:
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
            rj = r.json()[0]
            print "Error[{0}]:{1}. HTTP GET Request failed: {2} {3}".format(r.status_code,rj.get('error_code',""), rj.get('message',""), url)
            raise ValueError(r.status_code)
        
        return r.json()

    def httpPostRequest(self, url, data):

        _CFG = self.cfg

        authToken = self.getAuthToken()
        if not authToken:
            print "Error retrieving auth Token"

        if _CFG['debug']: log("HTTP POST: " + url)

        headersMap = {
            "Authorization": "Bearer " + authToken,
            "Content-Type": "application/json"
        }

        r = requests.post(url,data=json.dumps(data), headers=headersMap)

        

        if r.status_code != 200 and r.status_code != 201:
            rj = r.json()[0]
            print "Error[{0}]:{1}. HTTP POST Request failed: {2} {3}".format(r.status_code,rj.get('error_code',""), rj.get('message',""), url)
            raise ValueError(r.status_code)
        return r.json()


def createOathCredentials(name, description, permissionList, accountList, bcToken):

    data = {}
    data['type'] = 'credential'
    data['name'] = name
    #data['description'] = description
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

    log(json.dumps(data,indent=4, sort_keys=True))

    url = 'https://oauth.brightcove.com/v3/client_credentials'
    r = requests.post(url,data=json.dumps(data), headers=headersMap)

    if (r.status_code != 201):
        print "status code: {0}".format(r.status_code)
        print r.json()
        raise ValueError(r.status_code)

    results = r.json()
    
    print "name         : {0}".format(results.get('name',''))
    print "client id    : {0}".format(results.get('client_id',''))
    print "client secret: {0}".format(results.get('client_secret',''))

    print "status code: {0}".format(r.status_code)
    # if r.status_code != 200 and r.status_code != 201:
    #     print "status code: {0}".format(r.status_code)

    #     rj = r.json()[0]
    #     print "Error[{0}]:{1}. HTTP POST Request failed: {2} {3}".format(r.status_code,rj.get('error_code',""), rj.get('message',""), url)
    #     raise ValueError(r.status_code)
    return r.json()







def die(msg):
    print msg
    sys.exit()

def log(*args):
    sys.stderr.write(' '.join(map(str,args)) + '\n')
    sys.stderr.flush()



