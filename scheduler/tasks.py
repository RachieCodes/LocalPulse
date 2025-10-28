from scheduler.celery_app import app
from database.mongo_client import MongoDatabase
from utils.data_pipeline import DataPipeline
import logging
import subprocess
import sys
import os
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global database and pipeline instances
db = MongoDatabase()
pipeline = DataPipeline(db)


@app.task(bind=True, max_retries=3)
def scrape_yelp_businesses(self, category='restaurants', location='New York, NY', limit=50):
    """Scrape businesses from Yelp"""
    try:
        logger.info(f"Starting Yelp scraping: {category} in {location}")
        
        # Connect to database
        db.connect()
        
        # Run Scrapy spider
        scrapy_project_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scrapers')
        
        cmd = [
            'scrapy', 'crawl', 'yelp',
            '-a', f'category={category}',
            '-a', f'location={location}',
            '-s', f'CLOSESPIDER_ITEMCOUNT={limit}'
        ]
        
        result = subprocess.run(
            cmd,
            cwd=scrapy_project_path,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            logger.info(f"Yelp scraping completed successfully")
            return {"status": "success", "scraped_items": "unknown", "timestamp": datetime.now().isoformat()}
        else:
            logger.error(f"Yelp scraping failed: {result.stderr}")
            raise Exception(f"Scraping failed: {result.stderr}")
            
    except Exception as exc:
        logger.error(f"Yelp scraping task failed: {exc}")
        raise self.retry(exc=exc, countdown=300)  # Retry in 5 minutes
    finally:
        db.close()


@app.task(bind=True, max_retries=3)
def scrape_directory_businesses(self, category='restaurants', location='New York, NY', limit=30):
    """Scrape businesses from directory sites"""
    try:
        logger.info(f"Starting directory scraping: {category} in {location}")
        
        # Connect to database
        db.connect()
        
        # Run directory spider
        scrapy_project_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scrapers')
        
        cmd = [
            'scrapy', 'crawl', 'directory',
            '-a', f'category={category}',
            '-a', f'location={location}',
            '-s', f'CLOSESPIDER_ITEMCOUNT={limit}'
        ]
        
        result = subprocess.run(
            cmd,
            cwd=scrapy_project_path,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )
        
        if result.returncode == 0:
            logger.info(f"Directory scraping completed successfully")
            return {"status": "success", "scraped_items": "unknown", "timestamp": datetime.now().isoformat()}
        else:
            logger.error(f"Directory scraping failed: {result.stderr}")
            raise Exception(f"Scraping failed: {result.stderr}")
            
    except Exception as exc:
        logger.error(f"Directory scraping task failed: {exc}")
        raise self.retry(exc=exc, countdown=600)  # Retry in 10 minutes
    finally:
        db.close()


@app.task(bind=True, max_retries=2)
def process_new_reviews(self, limit=200):
    """Process new reviews with sentiment analysis"""
    try:
        logger.info(f"Starting review processing for {limit} reviews")
        
        # Connect to database
        db.connect()
        
        # Process reviews
        processed_count = pipeline.process_new_reviews(limit=limit)
        
        logger.info(f"Processed {processed_count} reviews")
        return {
            "status": "success",
            "processed_reviews": processed_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Review processing task failed: {exc}")
        raise self.retry(exc=exc, countdown=180)  # Retry in 3 minutes
    finally:
        db.close()


@app.task(bind=True, max_retries=2)
def update_business_analytics(self):
    """Update analytics for all businesses"""
    try:
        logger.info("Starting business analytics update")
        
        # Connect to database
        db.connect()
        
        # Update analytics
        updated_count = pipeline.update_business_analytics()
        
        logger.info(f"Updated analytics for {updated_count} businesses")
        return {
            "status": "success",
            "updated_businesses": updated_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Analytics update task failed: {exc}")
        raise self.retry(exc=exc, countdown=300)  # Retry in 5 minutes
    finally:
        db.close()


@app.task(bind=True, max_retries=2)
def generate_trending_keywords(self, days=7):
    """Generate trending keywords for specified period"""
    try:
        logger.info(f"Generating trending keywords for last {days} days")
        
        # Connect to database
        db.connect()
        
        # Generate keywords
        keywords = pipeline.generate_trending_keywords(days=days)
        
        logger.info(f"Generated {len(keywords)} trending keywords")
        return {
            "status": "success",
            "keywords_count": len(keywords),
            "period_days": days,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Trending keywords task failed: {exc}")
        raise self.retry(exc=exc, countdown=180)  # Retry in 3 minutes
    finally:
        db.close()


@app.task(bind=True, max_retries=2)
def detect_rating_anomalies(self):
    """Detect rating anomalies for all businesses"""
    try:
        logger.info("Starting rating anomaly detection")
        
        # Connect to database
        db.connect()
        
        # Get all businesses
        businesses = list(db.db.businesses.find({}, {"source_id": 1, "name": 1}))
        
        total_anomalies = 0
        businesses_with_anomalies = []
        
        for business in businesses[:100]:  # Limit to prevent timeout
            business_id = business.get("source_id")
            if not business_id:
                continue
                
            try:
                anomalies = pipeline.detect_rating_anomalies(business_id)
                
                if anomalies:
                    total_anomalies += len(anomalies)
                    businesses_with_anomalies.append({
                        "business_id": business_id,
                        "business_name": business.get("name"),
                        "anomalies": anomalies
                    })
                    
                    # Store anomalies in database
                    db.db.rating_anomalies.update_one(
                        {"business_id": business_id},
                        {
                            "$set": {
                                "business_name": business.get("name"),
                                "anomalies": anomalies,
                                "detected_at": datetime.now()
                            }
                        },
                        upsert=True
                    )
            except Exception as e:
                logger.warning(f"Failed to detect anomalies for business {business_id}: {e}")
                continue
        
        logger.info(f"Detected {total_anomalies} anomalies across {len(businesses_with_anomalies)} businesses")
        return {
            "status": "success",
            "total_anomalies": total_anomalies,
            "businesses_with_anomalies": len(businesses_with_anomalies),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Rating anomaly detection task failed: {exc}")
        raise self.retry(exc=exc, countdown=600)  # Retry in 10 minutes
    finally:
        db.close()


@app.task(bind=True)
def cleanup_old_data(self, days_to_keep=90):
    """Clean up old data to maintain database size"""
    try:
        logger.info(f"Starting data cleanup, keeping last {days_to_keep} days")
        
        # Connect to database
        db.connect()
        
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # Clean up old trending keywords
        result1 = db.db.trending_keywords.delete_many({
            "generated_at": {"$lt": cutoff_date}
        })
        
        # Clean up old anomaly detections
        result2 = db.db.rating_anomalies.delete_many({
            "detected_at": {"$lt": cutoff_date}
        })
        
        logger.info(f"Cleanup completed: removed {result1.deleted_count} keyword records, {result2.deleted_count} anomaly records")
        return {
            "status": "success",
            "deleted_keywords": result1.deleted_count,
            "deleted_anomalies": result2.deleted_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Data cleanup task failed: {exc}")
        raise
    finally:
        db.close()


# Additional utility tasks

@app.task
def health_check():
    """Simple health check task"""
    try:
        db.connect()
        # Simple database ping
        db.client.admin.command('ping')
        db.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


@app.task
def get_task_stats():
    """Get statistics about completed tasks"""
    try:
        # This would require Celery monitoring tools like Flower
        # For now, return basic info
        return {
            "status": "active",
            "timestamp": datetime.now().isoformat(),
            "message": "Task monitoring active"
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }