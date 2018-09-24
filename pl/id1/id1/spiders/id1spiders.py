
import scrapy
import time
from urllib.parse import unquote
from elasticsearch import Elasticsearch, RequestsHttpConnection


#host = 'search-tsai-t5aqxu4dppacep22fq5b4uvj6m.us-east-1.es.amazonaws.com'


es = Elasticsearch()

def insert(indexname, toInsert):
    res = es.index(index=indexname, doc_type='product',  body=toInsert)
    print(res)
    es.indices.refresh(index=indexname) 


class IDPricePriceSpider(scrapy.Spider):
    name = "IDPricePrice"

    limit =  12000000
    BASE_URL = 'http://id.priceprice.com'

    def start_requests(self):
        urls = [
            'http://id.priceprice.com/'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        if self.limit <= 0:
            return
        self.limit = self.limit -1

        page = response.url.split("/")[-2]
        filename = 'idpriceprice-%s.html' % page
        
        product = response.xpath('//div[@itemtype="http://schema.org/Product"]')
        keywords = response.xpath('/html/head/meta[@name="keywords"]/@content')[0].extract().split(",")
        imgUrl = response.xpath('/html/head/meta[@property="og:image"]/@content')[0].extract()
        itemName = response.xpath('//div[@class="itemSumName"]/div/h2[1]/text()').extract()
        rating = response.xpath('//div[@itemprop="aggregateRating"]/meta[@itemprop="ratingValue"]/@content').extract()
        
        currency = response.xpath('//meta[@itemprop="priceCurrency"]/@content').extract() 
        self.log("product schema should be only 1..it is now="+str(len(product)) )
        self.log("keywords...======== "+str(keywords))
        self.log("itemName...======== "+str(itemName))
        self.log("rating...======== "+str(rating))
        self.log("currency...======== "+str(currency))
        time.sleep(1)
        if len(product) > 0:
            self.log(product)        
            pname = product[0].xpath('./h1').extract()
            self.log("product name======================")
            self.log(pname)
            items = response.xpath('//div[@class="itemBox"]')
            for item in items:
                #self.log(item.extract())
                itemUrl = item.xpath('.//a[@class="shopBtn01"]/@href')
                itemPrice = item.xpath('.//p[@class="itmPrice_price"]/text()')
                if len(itemUrl) == 0:
                    continue 
                itemUrl = unquote(unquote(itemUrl[0].extract())).split("url=")[-1].split("?")[0].split("&hash")[0]
                itemShop = item.xpath('.//a[@class="itmShop_link"]/img/@alt')[0].extract()
                self.log("product link ======================")
                self.log(itemUrl)
                self.log("product price ======================")
                itemPrice = itemPrice[0].extract().replace("\n","").replace("Rp","") 
                self.log(itemPrice)
            
                oneItem = {'name':itemName[0],
                           'price':itemPrice,
                           'currency': currency,
                           'shop':itemShop,
                           'url':itemUrl,
                           'imgUrl':imgUrl,
                           'keywords':keywords,
                           'rating':rating
 
                          }
                yield self.insertItem(oneItem)
        
  
        allLinks = response.xpath('//a')
        for a in allLinks:
            if 'href' not in a.attrib:
                continue
            url = a.attrib['href']
            if not url.startswith("http"):
                self.log("---------> "+ str(self.limit)+" <------")
                self.log(url)
                absolute_url = self.BASE_URL + url
                yield scrapy.Request(absolute_url, callback=self.parse)

    def insertItem(self, oneItem):
        print(oneItem)
        self.log("===== in insert ====")
        insert("product",oneItem) 
        self.log("===== done insert ====")
        self.log(oneItem)
