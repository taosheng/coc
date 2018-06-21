#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import random
import boto3
import json
import urllib.parse
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

def searchProduct(queryString,minScore=1.7):

    queryString = queryString.strip()
    print(queryString)
    from storeList import stores
    store = ''
   # field = even['field']
   # idx = even['index']
    min_score = minScore
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
        if wordtype[1] in ['產品','商品','網路','商店','比價','便宜','查詢']:
            continue
        queryString = queryString+" "+wordtype[1]

    uid = even['uid']

    esSearchResult = searchProduct(queryString)
    if len(esSearchResult)  == 0:
    #try to find twice by simple cut words and lower down score
        print("do second search")
        secondQueryString = intent['msg']
        toIgnore = ['找便宜','便宜','商品比價','幫我比價','商品查詢','請幫我比價','找商品','網路商品','的']
        for ti in toIgnore:
            secondQueryString = secondQueryString.replace(ti, '')
        queryString = secondQueryString
        esSearchResult = searchProduct(queryString, minScore=1.3)

    resMsg = ""
    richContents = []
    tmpStoreList =[]
   
    richMsg = {
      "type": "flex",
      "altText": "product list",
      "contents":
         { "type": "carousel",
           "contents": [] ,
         }

      
       
    }


    for p in esSearchResult:
        storeDN = p['storeUrl'].split("//")[1].split("/")[0].replace("www.","").split(".")[0]
        if storeDN in tmpStoreList:
            continue
        tmpStoreList.append(storeDN)
        tmpDesc = {"type":"text", "text":"$"+str(p['price'])+","+p['product_name']}
        tmpImgUrl = "https://s3-us-west-2.amazonaws.com/scoc/"+p['image'] 
        tmpStoreUrl = p['storeUrl'].replace('http://','').replace('https://','')
        tmpButton = {
         "type": "button",
         "action": {
              "type": "uri",
              "label": "前往"+storeDN+"購買",
              "uri": 'https://'+urllib.parse.quote(tmpStoreUrl)
              #"uri": urllib.parse.quote(p['storeUrl'])
              #"uri": p['storeUrl']
           },
          
         "style": "primary",
        }
        itemBubble = {
          "type": "bubble",
          "hero": {
            "type": "image",
            "size": "full",
            "aspectRatio": "20:13",
            "aspectMode": "cover",
            "url": tmpImgUrl
            },
          "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [tmpDesc, tmpButton]
          },
        }
        richMsg['contents']['contents'].append(itemBubble)

#        tmpMsg = "價格{0}, {1}, 網址: {2} \n".format(p['price'], p['product_name'],p['storeUrl'])
        #resMsg = resMsg+tmpMsg

    if richMsg['contents']['contents']  == [] :
        resMsg = '目前找不到商品資訊:'+queryString
        toLineResponse={'uid':uid, 'msg':resMsg}
        lresponse = lambda_client.invoke(
             FunctionName='lineResponse',
             InvocationType='Event',
             LogType='None',
             ClientContext='string',
             Payload=json.dumps(toLineResponse),
        )
    else:
        toLineResponse={'uid':uid, 'msg':'', 'richMsg':richMsg}
        #print(toLineResponse)
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
    intent = {'timings': [], 'entities': ['便宜', '顯示器'], 'location': '', 'oriCut': [ ('n', '艾倫比亞健康面膜')], 'intent': '找', 'msg': '找便宜dyson吸塵器'}
    even = {'uid': 'Uc9b95e58acb9ab8d2948f8ac1ee48fad', 'callback': '', 'botid': '', 'msg': 'see intent ', 'intent': intent}
    r = lambda_scoclinehandler(even, None)
    #print("==== result ===")
    #for i in r:
    #    print(i) 


