#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import random
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from awsconfig import ESHOST, REGION
from nocheckin import aws_access_key_id,aws_secret_access_key

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


def lambda_scochandler(even, context):
# even structure
# {  'q': <product name>, 'store' : <specified store url>
#  }
#

    q = even['q']
    from storeList import stores
    store = ''
   # field = even['field']
   # idx = even['index']
    min_score = 2.0
    if "score" in even:
        min_score = int(even['score'])
    if "store" in even:
        store= even['store']

    q = {
      "min_score": min_score,
      "size" : 50,
      "query" :{
      "multi_match" : {
        "query": q,
        "fields": ['product_name']
      }
      }
    }

#    if store != '' :
    stores = [store]

    result = []
    for s in stores:
        res = es.search(index=s, body=q)

        for h in res['hits']['hits']:
            anItem = h['_source']
            anItem['store'] = s
            result.append(anItem)
    return result


if __name__ == '__main__':

    import sys
    q = sys.argv[1]
    even = {'q':q }
    r = lambda_scochandler(even, None)
    print("==== result ===")
    for i in r:
        print(i) 


