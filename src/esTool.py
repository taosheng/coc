#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime
import sys
import csv
import argparse
import json
from awsconfig import ESHOST, REGION
from nocheckin import aws_access_key_id, aws_secret_access_key

host = ESHOST
region = REGION

#host = 'search-tsai-t5aqxu4dppacep22fq5b4uvj6m.us-east-1.es.amazonaws.com'

awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key,region, 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

#es.indices.create(index="health")
#print("======= all upload ======")
def rebuild(indexname, jsonfile):
    es.indices.delete(index=indexname)
    es.indices.create(index=indexname)

    for line in open(jsonfile):
        s = line.strip()
        toInsert = eval(s)
        res = es.index(index=indexname, doc_type='fb',  body=toInsert)
        print(res)

    es.indices.refresh(index=indexname)

def delete(indexname, docid):
    es.delete(index=indexname, id=docid, doc_type='fb')
    es.indices.refresh(index=indexname)

def deleteIndice(index):
    es.indices.delete(index=index)

def listAllIndice(grep="."):
    totaldocs = 0
    for index in es.indices.get('*'):
        if grep not in index:
            continue
        numberDocs = es.count(index=index)['count']
        totaldocs += numberDocs
        print(index+" "+str(numberDocs))
    print(totaldocs)

def matchAll(indexname, query="", field=""):
    q ={
        "query":{

          "match_phrase": {
            field: query
          }

        }
    }
    res = es.search(index=indexname, body=q)
#print(res)
    print("Got %d Hits:" % res['hits']['total'])
    for h in res['hits']['hits']:
        print(h['_id']+ " "+ str(h['_source'])+ " "+str(h['_score']))

def listAll(indexname, query="", field=""):

    q = {
        "min_score": 0.3,
        "query" :{
          "multi_match" : {
              "query": query,
              "fields": [ field]
          } 
      },
         "size": 5000
    }


#    es.indices.refresh(index=indexname)
    res = es.search(index=indexname, body=q)
#print(res)
    print("Got %d Hits:" % res['hits']['total'])
    for h in res['hits']['hits']:
        print(h['_id']+ " "+ str(h['_source'])+ " "+str(h['_score']))

if __name__ == '__main__':
    bossid='Uc9b95e58acb9ab8d2948f8ac1ee48fad'
    parser = argparse.ArgumentParser(description='line user tool')
    parser.add_argument('--list','-l', action='store_true', help='list all docs')
    parser.add_argument('--rebuild','-r',action='store_true', help='rebuild index via upload a json file')
    parser.add_argument('--indexname','-i', help='index name')
    parser.add_argument('--query','-q', help='query string')
    parser.add_argument('--field','-f', help='query target field')
    parser.add_argument('--match','-m', action="store_true",help='exact match')
    parser.add_argument('--jsondump','-j', help='jsondump file name, one line per json doc')
    parser.add_argument('--delete','-d', action='store_true', help='to delete document')
    parser.add_argument('--docid','-c', help='document _id')
    parser.add_argument('--allindices','-a', action='store_true',help='list all indice')
    parser.add_argument('--deleteindices','-D', action='store_true',help='delete indice')

    args = parser.parse_args()

    if args.delete:
        print('to delete a single document')
        delete(args.indexname, args.docid)

    if args.list :
        print('query ...')
        if args.match:
            matchAll(args.indexname, args.query, args.field)
        else:
            listAll(args.indexname, args.query, args.field)
        exit(0)

    if args.rebuild:
        print('rebuild by jsonfile')
        rebuild(args.indexname, args.jsondump)

    if args.allindices :
        print('list all indices')
        listAllIndice()

    if args.deleteindices :
        print('delete indices')
        deleteIndice(args.indexname)
   

