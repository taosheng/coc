#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import random
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from awsconfig import ESHOST, REGION
from nocheckin import aws_access_key_id,aws_secret_access_key

lambda_client = boto3.client('lambda')

host = ESHOST
region = REGION


awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key, region, 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def searchProduct(queryString):

    queryString = queryString.strip()
    print(queryString)
    from storeList import stores
    store = ''
   # field = even['field']
   # idx = even['index']
    min_score = 1.7
    #if "score" in even:
    #    min_score = int(even['score'])
    #if "store" in even:
    #    store= even['store']

    q = {
      "min_score": min_score,
      "size" : 33,
      "query" :{
      "multi_match" : {
        "query": queryString,
        "fields": ['product_name']
      }
      }
    }

#    if store != '' :
    stores = [store]

    result = []
    for s in stores:
        res = es.search(index=s, body=q)

        print(res['hits']['hits'])
        for h in res['hits']['hits']:
            anItem = h['_source']
            anItem['store'] = s
            result.append(anItem)
    return result

def lambda_scoclinehandler(even, context):
#even structure {'uid',<line uid>, 'intent', {}}
    if 'intent' not in even or 'uid' not in even:
        print('no intent or uid')
        return 

    intent = even['intent']
    print(intent)
    queryString  = ''

    for wordtype in intent['oriCut']:
        if wordtype[0] in  ['v','r','a','uj','zg'] :
            continue
        if wordtype[1] in ['產品','商品','網路','商店','比價','便宜']:
            continue
        queryString = queryString+" "+wordtype[1]

    uid = even['uid']

    esSearchResult = searchProduct(queryString)
    resMsg = ""
    tmpStoreList =[]
    for p in esSearchResult:
#https://www.momomall.com.tw/s/102191/1021910000246/3000000000/
        storeDN = p['storeUrl'].split("//")[1].split("/")[0]
        if storeDN in tmpStoreList:
            continue
        tmpStoreList.append(storeDN)
        tmpMsg = "價格{0}, {1}, 網址: {2} \n".format(p['price'], p['product_name'],p['storeUrl'])
        resMsg = resMsg+tmpMsg

    if resMsg == '':
        resMsg = '目前找不到商品資訊:'+queryString
    toLineResponse={'uid':uid, 'msg':resMsg}

    lresponse = lambda_client.invoke(
         FunctionName='lineResponse',
         InvocationType='Event',
         LogType='None',
         ClientContext='string',
         Payload=json.dumps(toLineResponse),
    )



def lambda_scochandler(even, context):
# even structure
# {  'q': <product name>, 'store' : <specified store url>
#  }
#
    print(even)

    q = even['q']

    return searchProduct(q)

if __name__ == '__main__':

    import sys
    q = sys.argv[1]
    #even = {'q':q }
    #even = {'q':q }
    intent = {'entities': ['商品電冰箱', '電冰箱', '我', '商品'], 'timings': [], 'oriCut': [['v', '幫'], ['r', '我'], ['v', '找'], ['n', '商品'], ['n', '電冰箱']], 'intent': '找', 'location': '', 'msg': '幫我找商品電冰箱'}
    intent = {'timings': [], 'entities': ['便宜', '電視'], 'location': '', 'oriCut': [('v', '找'), ('a', '便宜'), ('uj', '的'), ('n', '電視')], 'intent': '找', 'msg': '找便宜的電視'}
    even = {'uid': 'Uc9b95e58acb9ab8d2948f8ac1ee48fad', 'callback': '', 'botid': '', 'msg': 'see intent ', 'intent': intent}
    r = lambda_scoclinehandler(even, None)
    #print("==== result ===")
    #for i in r:
    #    print(i) 


