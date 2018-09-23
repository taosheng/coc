
import scrapy


class IDPricePriceSpider(scrapy.Spider):
    name = "IDPricePrice"

    def start_requests(self):
        urls = [
            'http://id.priceprice.com/'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = 'idpriceprice-%s.html' % page
  
        allLinks = response.xpath('//a')
        for a in allLinks:
            url = a.attrib['href']
            if not url.startswith("http"):
                self.log(a.attrib['href'])
