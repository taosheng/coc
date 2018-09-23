
import scrapy


class IDPricePriceSpider(scrapy.Spider):
    name = "IDPricePrice"

    limit =  20
    BASE_URL = 'http://id.priceprice.com'

    def start_requests(self):
        urls = [
            'http://id.priceprice.com/OPPO-A3s-27132/'
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
        self.log("product schema should be only 1..it is now="+str(len(product)) )

        if len(product) > 0:
            self.log(product)        
            pname = product[0].xpath('./h1').extract()
            self.log("product name======================")
            self.log(pname)
            items = response.xpath('//div[@class="itemBox"]')
            for item in items:
                self.log(item)
                self.log("product link ======================")
                itemUrl = item.xpath('./a[@class="shopBtn01"]/@href')
                self.log(itemUrl)
            
        
  
        allLinks = response.xpath('//a')
        for a in allLinks:
            if 'href' not in a.attrib:
                continue
            url = a.attrib['href']
            if not url.startswith("http"):
                self.log("--------->"+ str(self.limit)+"<------")
                self.log(url)
                absolute_url = self.BASE_URL + url
                yield scrapy.Request(absolute_url, callback=self.parse)
