from pymongo import MongoClient, GEOSPHERE
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

load_dotenv()

class MongoDatabase:
    def __init__(self, uri=None, database_name=None):
        self.uri = uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        self.database_name = database_name or os.getenv('MONGO_DATABASE', 'localpulse')
        self.client = None
        self.db = None
        
    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.database_name]
            
            # Test connection
            self.client.admin.command('ping')
            logging.info(f"Connected to MongoDB: {self.database_name}")
            
            # Setup collections and indexes
            self._setup_collections()
            
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _setup_collections(self):
        """Create collections and indexes"""
        # Businesses collection
        businesses = self.db.businesses
        businesses.create_index([("location", GEOSPHERE)])
        businesses.create_index([("source_id", 1), ("source", 1)], unique=True)
        businesses.create_index([("category", 1)])
        businesses.create_index([("rating", -1)])
        businesses.create_index([("review_count", -1)])
        businesses.create_index([("name", "text"), ("description", "text")])
        
        # Reviews collection
        reviews = self.db.reviews
        reviews.create_index([("business_id", 1)])
        reviews.create_index([("review_date", -1)])
        reviews.create_index([("source_review_id", 1), ("source", 1)], unique=True)
        reviews.create_index([("sentiment_score", 1)])
        reviews.create_index([("review_text", "text")])
        
        # Events collection
        events = self.db.events
        events.create_index([("location", GEOSPHERE)])
        events.create_index([("date", 1)])
        events.create_index([("category", 1)])
        events.create_index([("source_id", 1), ("source", 1)], unique=True)
        
        logging.info("Database collections and indexes created")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logging.info("Database connection closed")
    
    def get_businesses_by_category(self, category, limit=50):
        """Get businesses by category"""
        return list(self.db.businesses.find(
            {"category": {"$regex": category, "$options": "i"}},
            limit=limit
        ).sort("rating", -1))
    
    def get_businesses_near_location(self, longitude, latitude, max_distance=5000, limit=50):
        """Get businesses near a location (distance in meters)"""
        return list(self.db.businesses.find({
            "location": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                    "$maxDistance": max_distance
                }
            }
        }, limit=limit))
    
    def get_top_rated_businesses(self, category=None, limit=20):
        """Get top rated businesses, optionally filtered by category"""
        query = {}
        if category:
            query["category"] = {"$regex": category, "$options": "i"}
        
        return list(self.db.businesses.find(query).sort([
            ("rating", -1), 
            ("review_count", -1)
        ]).limit(limit))
    
    def get_reviews_for_business(self, business_id, limit=100):
        """Get reviews for a specific business"""
        return list(self.db.reviews.find(
            {"business_id": business_id}
        ).sort("review_date", -1).limit(limit))
    
    def get_reviews_by_date_range(self, start_date, end_date, business_id=None):
        """Get reviews within a date range"""
        query = {
            "review_date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        if business_id:
            query["business_id"] = business_id
            
        return list(self.db.reviews.find(query).sort("review_date", -1))
    
    def get_business_analytics(self, business_id):
        """Get analytics for a specific business"""
        pipeline = [
            {"$match": {"business_id": business_id}},
            {"$group": {
                "_id": None,
                "avg_rating": {"$avg": "$rating"},
                "total_reviews": {"$sum": 1},
                "avg_sentiment": {"$avg": "$sentiment_score"},
                "latest_review": {"$max": "$review_date"},
                "earliest_review": {"$min": "$review_date"}
            }}
        ]
        
        result = list(self.db.reviews.aggregate(pipeline))
        return result[0] if result else {}
    
    def get_category_analytics(self, category):
        """Get analytics for a business category"""
        # Get businesses in category
        businesses = self.get_businesses_by_category(category, limit=1000)
        business_ids = [b.get('source_id') for b in businesses]
        
        pipeline = [
            {"$match": {"business_id": {"$in": business_ids}}},
            {"$group": {
                "_id": None,
                "avg_rating": {"$avg": "$rating"},
                "total_reviews": {"$sum": 1},
                "avg_sentiment": {"$avg": "$sentiment_score"}
            }}
        ]
        
        result = list(self.db.reviews.aggregate(pipeline))
        return result[0] if result else {}
    
    def get_trending_keywords(self, days=30, limit=50):
        """Get trending keywords from recent reviews"""
        from datetime import timedelta
        
        start_date = datetime.now() - timedelta(days=days)
        
        pipeline = [
            {"$match": {"review_date": {"$gte": start_date}}},
            {"$project": {"words": {"$split": ["$review_text", " "]}}},
            {"$unwind": "$words"},
            {"$group": {"_id": "$words", "count": {"$sum": 1}}},
            {"$match": {"_id": {"$not": {"$in": ["", "the", "and", "or", "but", "a", "an"]}}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        
        return list(self.db.reviews.aggregate(pipeline))


# Global database instance
db = MongoDatabase()