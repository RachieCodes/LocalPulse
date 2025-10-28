import pymongo
from datetime import datetime
import logging
from scrapers.items import BusinessItem, ReviewItem, EventItem


class ValidationPipeline:
    def process_item(self, item, spider):
        # Add timestamp
        item['last_updated'] = datetime.now()
        
        # Basic validation
        if isinstance(item, BusinessItem):
            if not item.get('name') or not item.get('address'):
                raise DropItem(f"Missing required fields in {item}")
        elif isinstance(item, ReviewItem):
            if not item.get('business_id') or not item.get('review_text'):
                raise DropItem(f"Missing required fields in {item}")
        
        return item


class MongoPipeline:
    collection_name = 'scraped_data'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "localpulse"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        
        # Create collections and indexes
        self.businesses = self.db['businesses']
        self.reviews = self.db['reviews']
        self.events = self.db['events']
        
        # Create indexes for better performance
        self.businesses.create_index([("source_id", 1), ("source", 1)], unique=True)
        self.businesses.create_index([("location", "2dsphere")])
        self.businesses.create_index([("category", 1)])
        self.reviews.create_index([("business_id", 1)])
        self.reviews.create_index([("review_date", 1)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        try:
            if isinstance(item, BusinessItem):
                # Create location field for geospatial queries
                if item.get('latitude') and item.get('longitude'):
                    item['location'] = {
                        'type': 'Point',
                        'coordinates': [float(item['longitude']), float(item['latitude'])]
                    }
                
                # Upsert business data
                self.businesses.update_one(
                    {'source_id': item['source_id'], 'source': item['source']},
                    {'$set': dict(item)},
                    upsert=True
                )
                
            elif isinstance(item, ReviewItem):
                self.reviews.update_one(
                    {'source_review_id': item['source_review_id'], 'source': item['source']},
                    {'$set': dict(item)},
                    upsert=True
                )
                
            elif isinstance(item, EventItem):
                if item.get('latitude') and item.get('longitude'):
                    item['location'] = {
                        'type': 'Point',
                        'coordinates': [float(item['longitude']), float(item['latitude'])]
                    }
                
                self.events.update_one(
                    {'source_id': item['source_id'], 'source': item['source']},
                    {'$set': dict(item)},
                    upsert=True
                )
                
        except Exception as e:
            logging.error(f"Error inserting item: {e}")
            
        return item