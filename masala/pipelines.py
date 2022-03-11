# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
import logging as lg


class MasalaPipeline:
    today_collection = "MasalaToday"
    completed_collection = "MasalaAll"

    
    t_item_c = 0
    u_item_c = 0
    n_item_c = 0
    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri
        # self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI')
            # mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client["MasalaData"]
        lg.warning(f'----------------------------[ 🕷 {spider.name} 🕷 ]-----------------------------')
        if spider.name == "mspy":
            try:
                self.db[self.today_collection].drop()
            except:
                # lg.warning(f'----------------------------[Started >> {spider.name}]-----------------------------')
                pass
        elif spider.name == "mspy":
            lg.warning(f'----------------------------[ 🕷 Latest Result Will be updated 🕷 ]-----------------------------')
            lg.warning(f'----------------------------[ 🕷           MasalaToday         🕷 ]-----------------------------')
        elif spider.name == "msallspy":
            lg.warning(f'----------------------------[ 🕷 All Result Will be updated 🕷 ]-----------------------------')
            lg.warning(f'----------------------------[ 🕷             MasalaAll      🕷 ]-----------------------------')
        else:
            lg.warning(f'--------------------------------[ 😐 Spider is DEAD 😐 ]------------------------------------------')
    
    def close_spider(self, spider):
        self.client.close()
        lg.info('--------------------------------------------------------------------------------------')
        lg.info(f'##---->Total Todays  Videos : {self.t_item_c}')
        lg.info(f'##---->Total New     Videos : {self.n_item_c}')
        lg.info(f'##---->Total Updated Videos : {self.u_item_c}')
        lg.info('--------------------------------------------------------------------------------------')
        self.t_item_c = 0
        self.n_item_c = 0 
        self.u_item_c = 0

    def process_item(self, item, spider):
        if spider.name=="mspy":
            tCollection = self.db[self.today_collection]
            tCollection.insert(item)
            self.t_item_c +=1
            return item
        
        elif spider.name=="msallspy":
            cCollection = self.db[self.completed_collection]
            item_lookup = cCollection.find({"vid":item["vid"]})
            if item_lookup.count()==1:
                if item_lookup[0]["src"] != item["src"]:
                    item["other_urls"].append(item_lookup[0]["src"])

                    cCollection.delete_one({"vid":item["vid"]})
                    cCollection.insert(item)
                    
                    lg.warning(f'Updated--->>{item["vid"]}')

                    self.u_item_c+=1
            else:
                cCollection.insert(item)
                lg.warning(f'Added--->>{item["vid"]}')
                self.n_item_c+=1
            return item
        else:
            lg.warning(f'Unsual Path--->>{item["vid"]}')
            return item
