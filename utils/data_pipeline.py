from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from database.mongo_client import MongoDatabase
from utils.nlp_processor import ReviewProcessor


class DataPipeline:
    """Main data processing pipeline"""
    
    def __init__(self, db: MongoDatabase):
        self.db = db
        self.review_processor = ReviewProcessor()
        
    def process_new_reviews(self, limit: int = 100) -> int:
        """Process reviews that haven't been analyzed yet"""
        # Find reviews without sentiment analysis
        unprocessed_reviews = list(self.db.db.reviews.find(
            {"sentiment_score": {"$exists": False}},
            limit=limit
        ))
        
        if not unprocessed_reviews:
            logging.info("No unprocessed reviews found")
            return 0
        
        logging.info(f"Processing {len(unprocessed_reviews)} reviews")
        
        # Process reviews
        processed_reviews = self.review_processor.process_reviews_batch(unprocessed_reviews)
        
        # Update database
        for review in processed_reviews:
            self.db.db.reviews.update_one(
                {"_id": review["_id"]},
                {"$set": {
                    "sentiment_score": review.get("sentiment_score"),
                    "sentiment_label": review.get("sentiment_label"),
                    "keywords": review.get("keywords"),
                    "phrases": review.get("phrases"),
                    "processed_at": datetime.now()
                }}
            )
        
        logging.info(f"Successfully processed {len(processed_reviews)} reviews")
        return len(processed_reviews)
    
    def update_business_analytics(self, business_id: str = None) -> int:
        """Update analytics for businesses"""
        if business_id:
            business_ids = [business_id]
        else:
            # Get all businesses
            businesses = list(self.db.db.businesses.find({}, {"source_id": 1}))
            business_ids = [b["source_id"] for b in businesses]
        
        updated_count = 0
        
        for bid in business_ids:
            analytics = self.db.get_business_analytics(bid)
            
            if analytics:
                # Update business with analytics
                self.db.db.businesses.update_one(
                    {"source_id": bid},
                    {"$set": {
                        "analytics": analytics,
                        "analytics_updated": datetime.now()
                    }}
                )
                updated_count += 1
        
        logging.info(f"Updated analytics for {updated_count} businesses")
        return updated_count
    
    def generate_trending_keywords(self, days: int = 7) -> List[Dict]:
        """Generate trending keywords for recent period"""
        start_date = datetime.now() - timedelta(days=days)
        
        # Get recent reviews
        recent_reviews = list(self.db.db.reviews.find(
            {"review_date": {"$gte": start_date}},
            {"review_text": 1, "sentiment_score": 1}
        ))
        
        if not recent_reviews:
            return []
        
        # Extract keywords
        keyword_data = self.review_processor.get_keyword_cloud_data(recent_reviews)
        
        # Store in database
        self.db.db.trending_keywords.delete_many({})  # Clear old data
        if keyword_data:
            self.db.db.trending_keywords.insert_many([
                {**kw, "generated_at": datetime.now(), "period_days": days}
                for kw in keyword_data
            ])
        
        return keyword_data
    
    def calculate_competitor_metrics(self, business_ids: List[str]) -> Dict[str, Any]:
        """Calculate competitive metrics for a group of businesses"""
        competitors_data = []
        
        for business_id in business_ids:
            business = self.db.db.businesses.find_one({"source_id": business_id})
            if not business:
                continue
            
            analytics = self.db.get_business_analytics(business_id)
            
            competitors_data.append({
                "business_id": business_id,
                "name": business.get("name"),
                "rating": business.get("rating", 0),
                "review_count": business.get("review_count", 0),
                "avg_sentiment": analytics.get("avg_sentiment", 0),
                "category": business.get("category"),
                "price_range": business.get("price_range")
            })
        
        # Calculate comparative metrics
        if competitors_data:
            avg_rating = sum(c["rating"] for c in competitors_data) / len(competitors_data)
            avg_reviews = sum(c["review_count"] for c in competitors_data) / len(competitors_data)
            avg_sentiment = sum(c["avg_sentiment"] for c in competitors_data) / len(competitors_data)
            
            return {
                "competitors": competitors_data,
                "market_averages": {
                    "rating": round(avg_rating, 2),
                    "review_count": int(avg_reviews),
                    "sentiment": round(avg_sentiment, 3)
                },
                "generated_at": datetime.now()
            }
        
        return {"competitors": [], "market_averages": {}}
    
    def detect_rating_anomalies(self, business_id: str, threshold: float = 0.5) -> List[Dict]:
        """Detect significant changes in ratings over time"""
        reviews = self.db.get_reviews_for_business(business_id, limit=500)
        
        if len(reviews) < 10:
            return []
        
        # Group reviews by month
        monthly_ratings = {}
        for review in reviews:
            if not review.get("review_date") or not review.get("rating"):
                continue
            
            month_key = review["review_date"].strftime("%Y-%m")
            if month_key not in monthly_ratings:
                monthly_ratings[month_key] = []
            monthly_ratings[month_key].append(review["rating"])
        
        # Calculate monthly averages
        monthly_averages = {
            month: sum(ratings) / len(ratings)
            for month, ratings in monthly_ratings.items()
            if len(ratings) >= 3  # At least 3 reviews per month
        }
        
        # Detect anomalies (significant drops/increases)
        anomalies = []
        sorted_months = sorted(monthly_averages.keys())
        
        for i in range(1, len(sorted_months)):
            prev_month = sorted_months[i-1]
            curr_month = sorted_months[i]
            
            prev_rating = monthly_averages[prev_month]
            curr_rating = monthly_averages[curr_month]
            
            change = curr_rating - prev_rating
            
            if abs(change) >= threshold:
                anomalies.append({
                    "month": curr_month,
                    "rating_change": round(change, 2),
                    "previous_rating": round(prev_rating, 2),
                    "current_rating": round(curr_rating, 2),
                    "type": "increase" if change > 0 else "decrease",
                    "severity": "high" if abs(change) >= 1.0 else "medium"
                })
        
        return anomalies
    
    def run_full_pipeline(self):
        """Run the complete data processing pipeline"""
        logging.info("Starting full data pipeline")
        
        try:
            # Process new reviews
            processed_reviews = self.process_new_reviews(limit=500)
            
            # Update business analytics
            updated_businesses = self.update_business_analytics()
            
            # Generate trending keywords
            trending_keywords = self.generate_trending_keywords(days=7)
            
            logging.info(f"Pipeline completed: {processed_reviews} reviews, {updated_businesses} businesses, {len(trending_keywords)} keywords")
            
            return {
                "processed_reviews": processed_reviews,
                "updated_businesses": updated_businesses,
                "trending_keywords_count": len(trending_keywords),
                "completed_at": datetime.now()
            }
            
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise