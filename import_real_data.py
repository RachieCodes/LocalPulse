#!/usr/bin/env python3
"""
Real Data Importer for LocalPulse
Fetches real businesses and reviews from Google Places API
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def import_real_businesses(cities=None, business_types=None):
    """Import real businesses and reviews from Google Places API"""
    try:
        from database.mongo_client import MongoDatabase
        from utils.new_places_api import NewPlacesAPISearch
        from utils.nlp_processor import ReviewProcessor
        
        db = MongoDatabase()
        db.connect()
        
        api = NewPlacesAPISearch()
        processor = ReviewProcessor()
        
        if not api.places_available:
            print("❌ Google Places API not available. Please check your API key.")
            return False
        
        # Default US cities to search
        if not cities:
            cities = [
                "Austin TX", "Portland OR", "Nashville TN", "Miami FL", 
                "Seattle WA", "Denver CO", "Boston MA", "Atlanta GA",
                "San Diego CA", "Chicago IL", "New York NY", "Los Angeles CA"
            ]
        
        # Default business types
        if not business_types:
            business_types = ["restaurant", "cafe", "retail", "service"]
        
        print(f"🔍 Searching for real businesses in {len(cities)} cities...")
        print(f"📋 Business types: {', '.join(business_types)}")
        
        total_businesses = 0
        total_reviews = 0
        
        for city in cities:
            print(f"\n🏙️ Processing {city}...")
            
            try:
                # Search for businesses in this city
                businesses = api.search_businesses_near_city(city, business_types, radius=10000)
                
                if not businesses:
                    print(f"   No businesses found in {city}")
                    continue
                
                print(f"   Found {len(businesses)} businesses")
                
                city_business_count = 0
                city_review_count = 0
                
                for business in businesses:
                    try:
                        # Convert business to database format
                        business_doc = {
                            "name": business.name,
                            "address": business.address,
                            "city": business.city,
                            "state": business.state,
                            "category": business.category,
                            "subcategory": business.subcategory,
                            "rating": business.rating,
                            "review_count": business.review_count,
                            "price_range": "$" * (business.price_level or 1),
                            "phone": business.phone,
                            "latitude": business.latitude,
                            "longitude": business.longitude,
                            "location": {"type": "Point", "coordinates": [business.longitude, business.latitude]},
                            "source": "google_places",
                            "source_id": business.place_id,
                            "place_id": business.place_id,
                            "last_updated": datetime.now()
                        }
                        
                        # Insert or update business
                        db.db.businesses.update_one(
                            {"source_id": business.place_id, "source": "google_places"},
                            {"$set": business_doc},
                            upsert=True
                        )
                        
                        city_business_count += 1
                        total_businesses += 1
                        
                        # Fetch reviews for this business
                        print(f"   📝 Fetching reviews for {business.name}...")
                        reviews = api.get_business_reviews(business.place_id)
                        
                        for review in reviews:
                            try:
                                # Convert review to database format
                                review_doc = {
                                    "business_id": business.place_id,
                                    "business_name": business.name,
                                    "business_city": business.city,
                                    "reviewer_name": review['reviewer_name'],
                                    "reviewer_id": f"google_{hash(review['reviewer_name'])}",
                                    "rating": review['rating'],
                                    "review_text": review['review_text'],
                                    "review_date": datetime.fromisoformat(review['review_date'].replace('Z', '+00:00')) if review['review_date'] else datetime.now(),
                                    "helpful_votes": review.get('helpful_votes', 0),
                                    "source": "google_places",
                                    "source_review_id": f"{business.place_id}_review_{hash(review['review_text'])}",
                                    "place_id": business.place_id,
                                    "sentiment_score": review.get('sentiment_score'),
                                    "sentiment_label": review.get('sentiment_label'),
                                    "keywords": review.get('keywords', []),
                                    "last_updated": datetime.now()
                                }
                                
                                # Enhanced sentiment analysis using NLP processor
                                if review['review_text']:
                                    processed = processor.process_review(review['review_text'])
                                    review_doc.update({
                                        "sentiment_score": processed.get("sentiment_score", review_doc['sentiment_score']),
                                        "sentiment_label": processed.get("sentiment_label", review_doc['sentiment_label']),
                                        "keywords": processed.get("keywords", review_doc['keywords']),
                                        "phrases": processed.get("phrases", [])
                                    })
                                
                                # Insert or update review
                                db.db.reviews.update_one(
                                    {"source_review_id": review_doc["source_review_id"], "source": "google_places"},
                                    {"$set": review_doc},
                                    upsert=True
                                )
                                
                                city_review_count += 1
                                total_reviews += 1
                                
                            except Exception as e:
                                print(f"     ❌ Error processing review: {e}")
                                continue
                        
                        # Rate limiting
                        time.sleep(0.5)  # Respect API limits
                        
                    except Exception as e:
                        print(f"   ❌ Error processing business {business.name}: {e}")
                        continue
                
                print(f"   ✅ {city}: {city_business_count} businesses, {city_review_count} reviews")
                
            except Exception as e:
                print(f"   ❌ Error processing city {city}: {e}")
                continue
        
        print(f"\n🎉 Import complete!")
        print(f"📊 Total: {total_businesses} businesses, {total_reviews} reviews")
        
        # Generate analytics
        print("\n📊 Generating analytics...")
        try:
            from utils.data_pipeline import DataPipeline
            pipeline = DataPipeline(db)
            
            # Update business analytics
            updated_businesses = pipeline.update_business_analytics()
            print(f"✅ Updated analytics for {updated_businesses} businesses")
            
            # Generate trending keywords
            keywords = pipeline.generate_trending_keywords(days=30)
            print(f"✅ Generated {len(keywords)} trending keywords")
            
        except Exception as e:
            print(f"⚠️ Analytics generation failed: {e}")
        
        db.close()
        
        print("\n🚀 Next steps:")
        print("1. Start the dashboard: streamlit run dashboard/main_dashboard.py --server.port 8503")
        print("2. Visit: http://localhost:8503")
        print("3. Explore real business data with reviews!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error importing real data: {e}")
        return False

def main():
    print("🌟 LocalPulse Real Data Importer")
    print("=" * 40)
    
    # Test database connection
    try:
        from database.mongo_client import MongoDatabase
        db = MongoDatabase()
        db.connect()
        print("✅ Database connection successful")
        db.close()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Please make sure MongoDB is running")
        return
    
    # Test Google Places API
    try:
        from utils.new_places_api import NewPlacesAPISearch
        api = NewPlacesAPISearch()
        if not api.places_available:
            print("❌ Google Places API not available")
            print("Please check your .env file and API key")
            return
        print("✅ Google Places API available")
    except Exception as e:
        print(f"❌ API test failed: {e}")
        return
    
    print("\n📋 This will fetch real business data including:")
    print("   • Real businesses from Google Places API")
    print("   • Actual customer reviews and ratings")
    print("   • Sentiment analysis on real reviews")
    print("   • Geographic data for mapping")
    print("   • Business categories and contact info")
    
    print("\n⚠️ Note: This will make API calls to Google Places")
    print("   Each city search uses multiple API calls")
    print("   Estimated API calls: ~50-100 total")
    
    response = input("\nProceed with real data import? (y/n): ").lower().strip()
    
    if response == 'y':
        # Ask for city selection
        print("\n🏙️ Select cities to import (default: 12 major US cities):")
        print("1. Use default cities (Austin, Portland, Nashville, etc.)")
        print("2. Enter custom cities")
        
        city_choice = input("Choice (1/2): ").strip()
        
        cities = None
        if city_choice == '2':
            city_input = input("Enter cities (comma-separated, e.g., 'Austin TX, Boston MA'): ")
            if city_input.strip():
                cities = [city.strip() for city in city_input.split(',')]
        
        if import_real_businesses(cities):
            print("\n✅ Success! You now have real business data and reviews.")
        else:
            print("\n❌ Failed to import real data.")
    else:
        print("Import cancelled.")

if __name__ == "__main__":
    main()