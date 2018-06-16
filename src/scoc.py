#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime
from lxml import etree, html  
import requests
import random
import time
import sys
import csv
import uuid
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
    print(indexname)
    if not es.indices.exists(index=indexname):
        es.indices.create(index=indexname)
        #es.indices.refresh(index=indexname)

def isExist(url):
    q = {
      "size" : 3,
      "query" :{
        "match_phrase": {
            "storeUrl": url
          }
      }
    }

    res = es.search(body=q)

    matchNo = len(res['hits']['hits'])
    #print(url)
    #print(matchNo)
    if matchNo == 0:
        return False

    for h in res['hits']['hits']:
        anItem = h['_source']
        if anItem['storeUrl'] == url.strip() :
            return True
        
    return False

def uploadImageToS3(url, imageId):
    r = requests.get(url, stream=True)
    obj = bucket.Object(imageId)
    bArray = None

    with r.raw as data:
        f = data.read()
        bArray = bytearray(f)

    obj.put(Body = bArray, ContentType='image/jpeg')

    obj.Acl().put(ACL='public-read')

def insertES(store, itemDoc):
    res = es.index(index=store, doc_type='product',  body=itemDoc)


def ezItemPageHandler(purl):
    res = requests.get(purl)
    res.encoding = 'utf-8'
    print(purl)
    pageDomRoot = etree.fromstring(res.content, etree.HTMLParser())
    productName = pageDomRoot.xpath("//h1[@class='product-name']/text()")[0].strip()
    productList = pageDomRoot.xpath("//li")
    print(productName)

    for product in productList :
        storeUrl = product.xpath(".//a[@target]/@href")
        if len(storeUrl) == 0 :
            continue
        storeUrl =storeUrl[0]
        if storeUrl.count("//") == 0:
            continue
        print("=====")
        print(storeUrl)
        price = product.xpath(".//span[@class='item-price']/text()")
        if len(price) == 0:
            continue
            price = 0
        else:
            price = int(price[0].replace(",","").replace("$",""))
        store = storeUrl.split("//")[1].split("/")[0]
        productImageUrl = pageDomRoot.xpath(".//a/span/img/@src")[0]
        fname = str(uuid.uuid4().int)+'.jpg'
        uploadImageToS3(productImageUrl,fname)
        itemDoc = { 'product_name':productName , 'image':fname , 'storeUrl':storeUrl , 'price':price}
        print(itemDoc)

        if isExist(storeUrl):
            print("to be ignore!!!!!!!!!")
            continue 
        else :
            print("to be insert?")
            insertES(store, itemDoc)
            time.sleep(2)


def feItemPageHandler(purl):
    res = requests.get(purl)
    res.encoding = 'utf8' 
    print('--- a product in a store ---')
    print(purl)
    pageDomRoot = etree.fromstring(res.content, etree.HTMLParser())
    productName = pageDomRoot.xpath("//*[@id='product_info']/h1[1]/text()")[0].strip()
    productList = pageDomRoot.xpath("//a[@class='product_link mod_table_cell separated price_container']")
    print(productName)
    productImageUrl = pageDomRoot.xpath("//img[@itemprop='image']/@src")[0].split("?")[0]
    print(productImageUrl)
    fname = str(uuid.uuid4().int)+'.jpg'
    uploadImageToS3(productImageUrl,fname)

    for product in productList :
        print("=====")
    #    print(product)
        storeUrl = product.xpath("./@data-url")[0].split("utm")[0].replace("&osm=feebee","")
        price = product.xpath(".//div[@class='price ellipsis']/text()")[0].replace(",","")
        store = storeUrl.split("//")[1].split("/")[0]
        print(store)
        itemDoc = { 'product_name':productName , 'image':fname , 'storeUrl':storeUrl , 'price':price}
        print(itemDoc)

        if isExist(storeUrl):
            print("to be ignore")
            continue 
        else :
            print("to be insert")
            time.sleep(random.randint(1,3))
       # insertES(store, itemDoc)


#TODO:not possible to abstract it?
def itemPageHandler(purl):
    res = requests.get(purl)
    res.encoding = 'utf8' 
    print('--- a product in a store ---')
    print(purl)
    pageDomRoot = etree.fromstring(res.content, etree.HTMLParser())  
    productName = pageDomRoot.xpath("//*[@id='product-name']/text()")[0].strip()
    productList = pageDomRoot.xpath("//tbody/tr")
    productImageUrl = pageDomRoot.xpath("//img[@itemprop='image']/@src")[0]
    print(productImageUrl)
    fname = str(uuid.uuid4().int)+'.jpg'
    uploadImageToS3(productImageUrl,fname)
    for product in productList :
        print("=====")
        storeUrl = product.xpath(".//a/@data-href")[0]
        store = storeUrl.split("//")[1].split("/")[0]
        createIndex(store)
        price = None 
        for p in product.xpath(".//strong/text()"):
            if p[0] == '$':
                price = int(p.replace("$","").replace(",",""))
                break
        print(storeUrl)
        print(price)
        
        itemDoc = { 'product_name':productName , 'image':fname , 'storeUrl':storeUrl , 'price':price}
        print(itemDoc)
        insertES(store, itemDoc)
    


def fromListToPage(lurl):
    res = requests.get(lurl)
    res.encoding = 'utf8' 
    pageDomRoot = etree.fromstring(res.content, etree.HTMLParser())  
    pages= pageDomRoot.xpath("//h4/a/@href")
    for p in pages:
        furl = 'https://biggo.com.tw'+p
        print(furl)
        itemPageHandler(furl)
        time.sleep(random.randint(3,13))

def fefromListToPage(lurl):
    res = requests.get(lurl)
    res.encoding = 'utf8' 
    pageDomRoot = etree.fromstring(res.content, etree.HTMLParser())  
    pages= pageDomRoot.xpath("//a[@class='link_ghost grid_shadow']/@href")
    for p in pages:
        furl = 'https://feebee.com.tw'+p
        print(furl)
        feItemPageHandler(furl)
        time.sleep(random.randint(3,13))
        

if __name__ == '__main__' :
    parser = argparse.ArgumentParser(description='coc tool')
    parser.add_argument('--target','-t', choices=['fe', 'bi','ez'])
    parser.add_argument('--pageUrl','-p', help='page contains list')
    parser.add_argument('--pages','-s', help='pagenumbers', type=int)

    args = parser.parse_args()
    pageActionsDict = {'fe':fefromListToPage, 'bi':fromListToPage, 'ez':ezItemPageHandler}
#    if args.target)
#    url = sys.argv[1]
#    fefromListToPage(url)
#    feItemPageHandler(url)
#    itemPageHandler(url)
    pageActionsDict[args.target](args.pageUrl)
    exit(0)

    try:
        for i in range(1,args.pages):
            print(args.pageUrl+str(i))
            pageActionsDict[args.target](args.pageUrl+str(i))
    except:
        print("Unexpected error:"+args.pageUrl+str(i))


#    print(dir(es.indices))
#    a= es.indices.exists(index="pchome.com.twxx")
#    print(a)




#print(es)
