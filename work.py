import requests
import xmltodict
import json
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

url = 'http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19InfStateJson'
queryParams = '?' + 'ServiceKey=1frUP9iTL1isTF%2BeWO2%2BUzTqP3CGpSvOF7hjhLUKM4Y4NeJ71YnhcxVRrw4Ya3EpfgOgMMI8XjvYbdiQyIIcLg%3D%3D&' + 'numOfRows=1000&' + 'startCreateDt=20200101&' + 'endCreateDt=20210331'

es = Elasticsearch(
    hosts=[{'host': "localhost", 'port': "9200"}]
)

response_body = (requests.get(url + queryParams).content)
dict_data = xmltodict.parse(response_body)
origin_data = dict_data['response']['body']['items']['item']


days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
documents = []
print(origin_data)
for data in origin_data:
   
   if not 'careCnt' in data: data['careCnt'] = 0
   if not 'clearCnt' in data: data['clearCnt'] = 0
   if not 'resutlNegCnt' in data: data['resutlNegCnt'] = 0  

   documents.append(
           {
                   "seq": int(data['seq']),
                   "careCnt": int(data['careCnt']),
                   "clearCnt": int(data['clearCnt']),
                   "deathCnt": int(data['deathCnt']),
                   "decideCnt": int(data['decideCnt']),
                   "examCnt": int(data['examCnt']),
                   "resutlNegCnt": int(data['resutlNegCnt']),
                   "createDt": datetime.strptime(str(data['createDt']), '%Y-%m-%d %H:%M:%S.%f'),
                   "weekday": days[datetime.strptime(str(data['createDt']), '%Y-%m-%d %H:%M:%S.%f').weekday()]
           }
   )
     
# https://gomguard.tistory.com/127
def c0(x):
  return x['seq']

lists = sorted(documents,key=c0,reverse=False)
es_documents = []
lastDiff = 0

for data in lists:

   if not 'careCnt' in data: data['careCnt'] = 0
   if not 'clearCnt' in data: data['clearCnt'] = 0
   if not 'resutlNegCnt' in data: data['resutlNegCnt'] = 0
    
   
   es_documents.append(
           {
              '_index': "covid-19-stat-v1",
              '_source': {
                   "careCnt": int(data['careCnt']),
                   "clearCnt": int(data['clearCnt']),
                   "deathCnt": int(data['deathCnt']),
                   "decideCnt": int(data['decideCnt']),
                   "decideDiff": int(data['decideCnt']) - lastDiff,
                   "examCnt": int(data['examCnt']),
                   "resutlNegCnt": int(data['resutlNegCnt']),
                   "createDt": data['createDt'],
                   "weekday": data['weekday']
              }
           }
   )
   lastDiff = int(data['decideCnt'])

helpers.bulk(es, es_documents)
print(es_documents) 
