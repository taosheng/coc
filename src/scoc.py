#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime
from lxml import etree, html  
import requests
import sys
import csv
import boto3
import argparse
import json
from awsconfig import ESHOST, REGION
from nocheckin import aws_access_key_id, aws_secret_access_key

host = ESHOST
region = REGION


awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key,region, 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
s3 = boto3.resource('s3')
bucket = s3.Bucket('scoc')

def createIndex(indexname):
    if not es.indices.exists(index=indexname):
        es.indices.create(index=indexname)
        es.indices.refresh(index=indexname)

def uploadImageToS3(url, imageId):
    r = requests.get(url, stream=True)
    obj = bucket.Object(imageId)
    bArray = None

    with r.raw as data:
        f = data.read()
        bArray = bytearray(f)

    obj.put(Body = bArray, ContentType='image/jpeg')

    obj.Acl().put(ACL='public-read')



#TODO:not possible to abstract it?
def itemPageHandler(purl):
    res = requests.get(purl)
    res.encoding = 'utf8' 
    pageDomRoot = etree.fromstring(res.content, etree.HTMLParser())  
    productName = pageDomRoot.xpath("//*[@id='product-name']/text()")[0].strip()
    productList = pageDomRoot.xpath("//tbody/tr")
    productImageUrl = pageDomRoot.xpath("//img[@itemprop='image']/@src")[0]
    print(productImageUrl)
    uploadImageToS3(productImageUrl,'test2.jpg')
    for product in productList :
        print("=====")
        storeUrl = product.xpath(".//a/@data-href")[0]
        price = ''
        for p in product.xpath(".//strong/text()"):
            if p[0] == '$':
                price = p
                break
        print(storeUrl)
        print(price)
        
    


if __name__ == '__main__' :
    url = sys.argv[1]
    itemPageHandler(url)




#print(es)
