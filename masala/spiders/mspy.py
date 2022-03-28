import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import hashlib
from datetime import datetime
from pytz import timezone 

class MspySpider(CrawlSpider):
    name = 'mspy'
    allowed_domains = ['masahub.net']
    start_urls = ['http://masahub.net/']
    last_update = datetime.now(timezone("Asia/Kolkata")).strftime("%Y-%m-%dT%H:%M:%S")

    rules = (
        Rule(LinkExtractor(restrict_xpaths="//li[@class='thumi']/a[1]"), callback='parse_item', follow=False),
        # Rule(LinkExtractor(restrict_xpaths="//a[@class='next page-numbers']"))
    )

    def parse_item(self, response):
        # print(">>>>>>>>>>>>>>>>>>>")
        if response.url != "https://masahub.net/gallery/cute-sexy-girl-hot-pic-album/":
            title = response.xpath("//div[@id='sp']/b/text()").get()
            src_link = response.xpath("//video[@id='video-id']/source/@src").get()
            tags = response.xpath("//a[@rel='tag']/text()").getall()
            vid = hashlib.sha256(((title+src_link).lower()).encode()).hexdigest()

            yield{
                "vid" : vid,
                "title":title,
                "main_src":response.url,
                "src":src_link,
                "tags" : tags,
                "other_urls" : [],
                "last_update" : self.last_update
            }
        else:
            pass

