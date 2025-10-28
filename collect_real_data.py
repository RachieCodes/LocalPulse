#!/usr/bin/env python3
"""
LocalPulse Data Collection Helper

This script helps you set up and test real data collection.
"""

import sys
import os
import subprocess
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_mongodb():
    """Test MongoDB connection"""
    try:
        import pymongo
        client = pymongo.MongoClient('mongodb://localhost:27017')
        client.admin.command('ping')
        print("✅ MongoDB connection successful")
        client.close()
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("💡 Make sure MongoDB is running:")
        print("   Docker: docker run -d -p 27017:27017 mongo")
        print("   Native: mongod")
        return False

def test_redis():
    """Test Redis connection"""
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✅ Redis connection successful")
        return True
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("💡 Make sure Redis is running:")
        print("   Docker: docker run -d -p 6379:6379 redis")
        print("   WSL: redis-server")
        return False

def initialize_database():
    """Initialize the database with indexes"""
    try:
        from database.mongo_client import MongoDatabase
        db = MongoDatabase()
        db.connect()
        print("✅ Database initialized with proper indexes")
        db.close()
        return True
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

def run_sample_scraping():
    """Run a small scraping job to get sample data"""
    print("🕷️ Starting sample data collection...")
    
    try:
        # Change to scrapers directory
        scrapers_dir = project_root / "scrapers"
        
        # Run Yelp scraper with limited items
        cmd = [
            sys.executable, "-m", "scrapy", "crawl", "yelp",
            "-a", "category=restaurants",
            "-a", "location=New York, NY",
            "-s", "CLOSESPIDER_ITEMCOUNT=10"  # Limit to 10 items for testing
        ]
        
        print(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            cwd=scrapers_dir,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ Sample scraping completed successfully")
            print("📊 Check your dashboard for new data!")
            return True
        else:
            print(f"❌ Scraping failed:")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("⏰ Scraping timed out - this is normal for first runs")
        return False
    except Exception as e:
        print(f"❌ Scraping error: {e}")
        return False

def process_data():
    """Process collected data with NLP"""
    try:
        from utils.data_pipeline import DataPipeline
        from database.mongo_client import MongoDatabase
        
        db = MongoDatabase()
        db.connect()
        pipeline = DataPipeline(db)
        
        # Process reviews
        print("🧠 Processing reviews with sentiment analysis...")
        processed_reviews = pipeline.process_new_reviews(limit=50)
        print(f"✅ Processed {processed_reviews} reviews")
        
        # Update analytics
        print("📊 Updating business analytics...")
        updated_businesses = pipeline.update_business_analytics()
        print(f"✅ Updated analytics for {updated_businesses} businesses")
        
        # Generate keywords
        print("🏷️ Generating trending keywords...")
        keywords = pipeline.generate_trending_keywords(days=30)
        print(f"✅ Generated {len(keywords)} trending keywords")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Data processing failed: {e}")
        return False

def show_data_summary():
    """Show summary of collected data"""
    try:
        from database.mongo_client import MongoDatabase
        
        db = MongoDatabase()
        db.connect()
        
        # Count documents
        business_count = db.db.businesses.count_documents({})
        review_count = db.db.reviews.count_documents({})
        
        print(f"\n📈 Data Summary:")
        print(f"  Businesses: {business_count}")
        print(f"  Reviews: {review_count}")
        
        if business_count > 0:
            # Show sample business
            sample_business = db.db.businesses.find_one()
            print(f"  Sample business: {sample_business.get('name', 'Unknown')}")
        
        if review_count > 0:
            # Show processed reviews
            processed_reviews = db.db.reviews.count_documents({"sentiment_score": {"$exists": True}})
            print(f"  Processed reviews: {processed_reviews}")
        
        db.close()
        return business_count > 0 or review_count > 0
        
    except Exception as e:
        print(f"❌ Error checking data: {e}")
        return False

def main():
    print("🚀 LocalPulse Real Data Setup")
    print("=" * 40)
    
    # Test connections
    print("1. Testing database connections...")
    mongodb_ok = test_mongodb()
    redis_ok = test_redis()
    
    if not mongodb_ok:
        print("\n❌ MongoDB is required for data collection.")
        print("Please start MongoDB and try again.")
        return
    
    if not redis_ok:
        print("\n⚠️ Redis is optional but recommended for scheduling.")
    
    # Initialize database
    print("\n2. Initializing database...")
    if not initialize_database():
        print("❌ Database initialization failed.")
        return
    
    # Check existing data
    print("\n3. Checking existing data...")
    has_data = show_data_summary()
    
    if not has_data:
        print("\n4. No data found. Would you like to collect sample data?")
        response = input("Collect sample data? (y/n): ").lower().strip()
        
        if response == 'y':
            print("\n🕷️ Collecting sample data...")
            if run_sample_scraping():
                print("\n5. Processing collected data...")
                process_data()
                print("\n6. Final data summary:")
                show_data_summary()
    else:
        print("\n4. Data found! Processing existing data...")
        process_data()
    
    print("\n🎉 Setup complete!")
    print("\nNext steps:")
    print("1. Start the full dashboard: python start_dashboard.py")
    print("2. For automated collection: python start_scheduler.py start")
    print("3. View your data at: http://localhost:8501")

if __name__ == "__main__":
    main()