"""
@author: ngarcia
Resources: https://developers.refinitiv.com/datascope-select-dss/datascope-select-rest-api/learning
DSS HTTP status codes: https://hosted.datascopeapi.reuters.com/RestApi.Help/Home/StatusCodes
"""

import datetime as dt
import json as js
import logging
import requests as rq
import sys

_un,_pw = "9001698","Axioma01"
_headers = {'Prefer':'respond-async','Content-Type': 'application/json; odata.metadata=minimal'}
_credentials={'Credentials':{ 'Password':_pw,'Username':_un } }

class DssException(Exception):
  # Custom Exception class
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return 'DSS api returned error {0}'.format(self.parameter)

def getAuthToken(un,pw,headers,credentials):
    urlAuthToken = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'
    i=0
    while i < 5:
      #Retry an arbitrary number of times before exiting
      _resp = rq.post(urlAuthToken, json=_credentials, headers=_headers )
      if _resp.status_code == 200:
        _jResp = _resp.json()
        return _jResp.get('value')
      elif _resp.status_code == 504:
        i+=1
        logging.info('DSS Gateway Time-out. \n Sleeping 2 seconds. ')
        time.sleep(2)
        logging.info('Attempt #{}. Retrying now.'.format(i))
        continue 
      else:
        logging.error('Encountered Error Status Code {0}, \n when getting Authorization Token /n{1}'.format(_resp.status_code,_resp.reason))
        raise DssException(_resp.text)

def getAllReportTemplates(header):
    reportTemplatesURL = 'https://hosted.datascopeapi.reuters.com/ServiceLayer/Extractions/ReportTemplates'
    resp = rq.get(reportTemplatesURL, headers=header)
    if resp.status_code == 200:
        result = resp.json()
        numElements = len(result["value"])
        logging.info(' Found {} Report Templates.'.format(numElements))
        return result
    else:
        logging.error(' Failed to get Report Templates.\n{0} Error. {1}.'.format(resp.status_code,resp.reason))
        raise DssException(_resp.text)

def getSpecificReport(reportsToFind,allReportTemplates,header):
  # Parses getAllReportTemplates() results for specific reports
  reports = reportsToFind.lower().split(',')
  if 'value' in allReportTemplates.keys():
    matches = [r for r in allReportTemplates['value'] if r.get('Name').lower() in reports]
    print(' Found {} Report Templates'.format(len(matches)))
    results = list(zip(reports,matches))
    return results
  else:
    return 'No values in {}'.format(reports)

def getExtractedFiles(header):
    logging.info(" Getting extracted files...")  
    urlGetExtractedFiles = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractedFiles'
    r = rq.get(urlGetExtractedFiles, headers=header)
    if r.status_code == 200:
      # Find TR ZeroCurves      
      # zeroCurves = [curve for curve in output['value'] if 'ZeroCurve' in curve['ExtractedFileName'] and curve['FileType'] == 'Full']
      return r.json()
    else:
      logging.error("{0}. Status_code={1} \n".format(r.reason,r.status_code))
      raise DssException(r.text)

def getExtractedFileIds(header):
    logging.info(" Getting extracted files_ids...")    
    urlGetExtractedIds = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ReportExtractions'
    r = rq.get(urlGetExtractedIds, headers=header)
    if r.status_code == 200:
      output = r.json()
      return output
    else:
      logging.error("{0}. Status_code={1} \n".format(r.reason,r.status_code))
      raise DssException(r.text)

def getSchedules(header):
  logging.info(" Getting all report schedules...")  
  urlSchedules = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Schedules'
  r = rq.get(urlSchedules, headers=header)
  if r.status_code == 200:
    output = r.json()
    return output
  else:
    logging.error("{0}. Status_code={1} \n".format(r.reason,r.status_code))
    raise DssException(r.text)

def timeNow():
    return dt.datetime.strftime(dt.datetime.now(), "%c")

def main():
    # get session token
    authToken = getAuthToken(_un,_pw,_headers,_credentials)
    myToken = 'Token '+authToken
    currDT = timeNow() + '_test'
    # X-Client-Session-Id is unique ID for this transaction to the DSS server
    headers = {'Content-Type': 'application/json; odata.metadata=minimal', 'X-Client-Session-Id': currDT, 'Authorization': myToken}
    logging.info('Successfully created token: {}'.format(myToken))
    #allReportTemps = getAllReportTemplates(headers)
    #print(allReportTemps)
    #reportsToFind = 'Omer_test,omer_test,20150205_Aakaash,nick_test'
    #specificReports = getSpecificReport(reportsToFind,allReportTemps,headers)
    #print(specificReports)
    #res = lookup_test(headers)
    #print(res)
    #r = getExtractedFiles(headers)
    #print(r)
    #r = getExtractedFileIds(headers)
    r = getSchedules(headers)
    print(r)

if __name__ == "__main__":
  main()

