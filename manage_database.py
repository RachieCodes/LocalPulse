#!/usr/bin/env python3
"""
Database Management Utility for LocalPulse
Clean, reset, or manage database entries
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def clear_all_data():
    """Clear all data from the database"""
    try:
        from database.mongo_client import MongoDatabase
        
        db = MongoDatabase()
        db.connect()
        
        print("🗑️ Clearing all database data...")
        
        # Count current entries
        business_count = db.db.businesses.count_documents({})
        review_count = db.db.reviews.count_documents({})
        analytics_count = db.db.analytics.count_documents({})
        keywords_count = db.db.trending_keywords.count_documents({})
        
        print(f"📊 Current data:")
        print(f"   • Businesses: {business_count}")
        print(f"   • Reviews: {review_count}")
        print(f"   • Analytics: {analytics_count}")
        print(f"   • Keywords: {keywords_count}")
        
        # Clear collections
        result1 = db.db.businesses.delete_many({})
        result2 = db.db.reviews.delete_many({})
        result3 = db.db.analytics.delete_many({})
        result4 = db.db.trending_keywords.delete_many({})
        
        print(f"\n✅ Deleted:")
        print(f"   • {result1.deleted_count} businesses")
        print(f"   • {result2.deleted_count} reviews")
        print(f"   • {result3.deleted_count} analytics")
        print(f"   • {result4.deleted_count} keywords")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Error clearing database: {e}")
        return False

def clear_by_source(source_name):
    """Clear data from a specific source (e.g., 'manual', 'google_places')"""
    try:
        from database.mongo_client import MongoDatabase
        
        db = MongoDatabase()
        db.connect()
        
        print(f"🗑️ Clearing data from source: {source_name}")
        
        # Count current entries for this source
        business_count = db.db.businesses.count_documents({"source": source_name})
        review_count = db.db.reviews.count_documents({"source": source_name})
        
        print(f"📊 Found {business_count} businesses and {review_count} reviews from '{source_name}'")
        
        if business_count == 0 and review_count == 0:
            print(f"⚠️ No data found for source '{source_name}'")
            db.close()
            return True
        
        # Delete by source
        result1 = db.db.businesses.delete_many({"source": source_name})
        result2 = db.db.reviews.delete_many({"source": source_name})
        
        print(f"\n✅ Deleted:")
        print(f"   • {result1.deleted_count} businesses")
        print(f"   • {result2.deleted_count} reviews")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Error clearing source data: {e}")
        return False

def clear_by_city(city_name):
    """Clear data from a specific city"""
    try:
        from database.mongo_client import MongoDatabase
        
        db = MongoDatabase()
        db.connect()
        
        print(f"🗑️ Clearing data from city: {city_name}")
        
        # Build query to match city in address or city field
        city_query = {
            "$or": [
                {"city": {"$regex": city_name, "$options": "i"}},
                {"address": {"$regex": city_name, "$options": "i"}}
            ]
        }
        
        review_query = {
            "$or": [
                {"business_city": {"$regex": city_name, "$options": "i"}},
                {"business_name": {"$in": []}}  # Will be populated below
            ]
        }
        
        # Get business names for this city
        businesses = list(db.db.businesses.find(city_query, {"name": 1}))
        business_names = [b["name"] for b in businesses]
        review_query["$or"][1]["business_name"]["$in"] = business_names
        
        # Count current entries
        business_count = len(businesses)
        review_count = db.db.reviews.count_documents(review_query) if business_names else 0
        
        print(f"📊 Found {business_count} businesses and {review_count} reviews in '{city_name}'")
        
        if business_count == 0:
            print(f"⚠️ No data found for city '{city_name}'")
            db.close()
            return True
        
        # Delete by city
        result1 = db.db.businesses.delete_many(city_query)
        result2 = db.db.reviews.delete_many(review_query) if business_names else type('obj', (object,), {'deleted_count': 0})()
        
        print(f"\n✅ Deleted:")
        print(f"   • {result1.deleted_count} businesses")
        print(f"   • {result2.deleted_count} reviews")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Error clearing city data: {e}")
        return False

def show_database_stats():
    """Show current database statistics"""
    try:
        from database.mongo_client import MongoDatabase
        
        db = MongoDatabase()
        db.connect()
        
        print("📊 Database Statistics:")
        print("=" * 30)
        
        # Overall counts
        business_count = db.db.businesses.count_documents({})
        review_count = db.db.reviews.count_documents({})
        analytics_count = db.db.analytics.count_documents({})
        keywords_count = db.db.trending_keywords.count_documents({})
        
        print(f"📈 Total Entries:")
        print(f"   • Businesses: {business_count:,}")
        print(f"   • Reviews: {review_count:,}")
        print(f"   • Analytics: {analytics_count:,}")
        print(f"   • Keywords: {keywords_count:,}")
        
        if business_count > 0:
            # By source
            print(f"\n📂 By Source:")
            sources = db.db.businesses.distinct("source")
            for source in sources:
                b_count = db.db.businesses.count_documents({"source": source})
                r_count = db.db.reviews.count_documents({"source": source})
                print(f"   • {source}: {b_count} businesses, {r_count} reviews")
            
            # By city (top 10)
            print(f"\n🏙️ Top Cities:")
            cities_pipeline = [
                {"$group": {"_id": "$city", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            cities = list(db.db.businesses.aggregate(cities_pipeline))
            for city in cities:
                city_name = city["_id"] or "Unknown"
                print(f"   • {city_name}: {city['count']} businesses")
            
            # By category
            print(f"\n🏪 By Category:")
            categories = db.db.businesses.distinct("category")
            for category in categories:
                count = db.db.businesses.count_documents({"category": category})
                print(f"   • {category}: {count} businesses")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Error getting database stats: {e}")
        return False

def backup_database():
    """Create a simple backup of the database"""
    try:
        from database.mongo_client import MongoDatabase
        import json
        
        db = MongoDatabase()
        db.connect()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        
        print(f"💾 Creating database backup...")
        
        # Backup each collection
        collections = ["businesses", "reviews", "analytics", "trending_keywords"]
        backup_files = []
        
        for collection_name in collections:
            collection = getattr(db.db, collection_name)
            data = list(collection.find({}))
            
            if data:
                # Convert ObjectId to string for JSON serialization
                for item in data:
                    if '_id' in item:
                        item['_id'] = str(item['_id'])
                    if 'last_updated' in item and hasattr(item['last_updated'], 'isoformat'):
                        item['last_updated'] = item['last_updated'].isoformat()
                    if 'review_date' in item and hasattr(item['review_date'], 'isoformat'):
                        item['review_date'] = item['review_date'].isoformat()
                
                backup_file = backup_dir / f"{collection_name}_{timestamp}.json"
                with open(backup_file, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
                
                backup_files.append(backup_file)
                print(f"   ✅ {collection_name}: {len(data)} records → {backup_file}")
        
        db.close()
        
        if backup_files:
            print(f"\n💾 Backup completed!")
            print(f"📁 Files saved in: {backup_dir.absolute()}")
        else:
            print("⚠️ No data to backup")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return False

def main():
    print("🛠️ LocalPulse Database Management")
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
    
    while True:
        print("\n📋 Available Actions:")
        print("1. 📊 Show database statistics")
        print("2. 💾 Create database backup")
        print("3. 🗑️ Clear all data")
        print("4. 🔍 Clear data by source (manual/google_places)")
        print("5. 🏙️ Clear data by city")
        print("6. 🚪 Exit")
        
        choice = input("\nSelect an action (1-6): ").strip()
        
        if choice == '1':
            show_database_stats()
            
        elif choice == '2':
            backup_database()
            
        elif choice == '3':
            print("\n⚠️ WARNING: This will delete ALL data!")
            confirm = input("Type 'DELETE ALL' to confirm: ").strip()
            if confirm == 'DELETE ALL':
                if clear_all_data():
                    print("✅ All data cleared successfully")
                else:
                    print("❌ Failed to clear data")
            else:
                print("❌ Action cancelled")
                
        elif choice == '4':
            print("\nAvailable sources:")
            print("• manual - Sample data created by create_sample_data.py")
            print("• google_places - Real data from Google Places API")
            source = input("Enter source name: ").strip()
            if source:
                if clear_by_source(source):
                    print(f"✅ Data from '{source}' cleared successfully")
                else:
                    print(f"❌ Failed to clear data from '{source}'")
            else:
                print("❌ No source specified")
                
        elif choice == '5':
            city = input("Enter city name (e.g., 'Austin', 'New York'): ").strip()
            if city:
                if clear_by_city(city):
                    print(f"✅ Data from '{city}' cleared successfully")
                else:
                    print(f"❌ Failed to clear data from '{city}'")
            else:
                print("❌ No city specified")
                
        elif choice == '6':
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please select 1-6.")

if __name__ == "__main__":
    main()