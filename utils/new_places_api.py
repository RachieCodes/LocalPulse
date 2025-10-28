"""
Enhanced location search using Google's NEW Places API
"""

import os
from typing import List, Optional, Dict, Any
import requests
import json
import time
import random
from datetime import datetime
import logging
from dataclasses import dataclass

@dataclass
class BusinessResult:
    """Structure for business search results"""
    name: str
    address: str
    city: str
    state: str
    rating: float
    review_count: int
    category: str
    subcategory: List[str]
    phone: Optional[str]
    latitude: float
    longitude: float
    place_id: str
    price_level: Optional[int]

class NewPlacesAPISearch:
    """Search for real businesses using Google's NEW Places API"""
    
    def __init__(self, api_key: Optional[str] = None):
        from dotenv import load_dotenv
        load_dotenv()
        
        self.api_key = api_key or os.getenv('GOOGLE_PLACES_API_KEY')
        self.geocoding_available = False
        self.places_available = False
        
        # Test API availability
        if self.api_key:
            self._test_apis()
    
    def _test_apis(self):
        """Test if both Geocoding and Places APIs are working"""
        try:
            # Test Geocoding API
            geocode_url = "https://maps.googleapis.com/maps/api/geocode/json"
            geocode_response = requests.get(geocode_url, params={
                'address': 'New York, NY',
                'key': self.api_key
            })
            
            if geocode_response.status_code == 200:
                geocode_data = geocode_response.json()
                self.geocoding_available = geocode_data.get('status') == 'OK'
            
            # Test NEW Places API
            places_url = "https://places.googleapis.com/v1/places:searchText"
            places_headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.api_key,
                'X-Goog-FieldMask': 'places.displayName'
            }
            places_response = requests.post(places_url, json={
                "textQuery": "test restaurant",
                "maxResultCount": 1
            }, headers=places_headers)
            
            self.places_available = places_response.status_code == 200
            
            if self.geocoding_available and self.places_available:
                logging.info("Google APIs are working - real data mode enabled")
            else:
                logging.warning("Google APIs not fully available - using demo mode")
                
        except Exception as e:
            logging.error(f"API test failed: {e}")
            self.geocoding_available = False
            self.places_available = False
    
    @property
    def gmaps(self):
        """Compatibility property for dashboard"""
        return self.places_available and self.geocoding_available
    
    def geocode_city(self, city_query: str) -> Optional[Dict]:
        """Get coordinates for a city name using Geocoding API"""
        if not self.geocoding_available:
            return self._get_demo_coordinates(city_query)
        
        try:
            geocode_url = "https://maps.googleapis.com/maps/api/geocode/json"
            response = requests.get(geocode_url, params={
                'address': city_query,
                'key': self.api_key
            })
            
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'OK' and data['results']:
                    location = data['results'][0]
                    return {
                        'lat': location['geometry']['location']['lat'],
                        'lng': location['geometry']['location']['lng'],
                        'formatted_address': location['formatted_address']
                    }
        except Exception as e:
            logging.error(f"Geocoding error: {e}")
        
        return self._get_demo_coordinates(city_query)
    
    def search_businesses_near_city(self, city_query: str, business_types: List[str] = None, radius: int = 5000) -> List[BusinessResult]:
        """Search for businesses near a city using NEW Places API"""
        if not self.places_available:
            return self._get_demo_businesses(city_query)
        
        businesses = []
        
        # Get coordinates for the city first
        coordinates = self.geocode_city(city_query)
        if not coordinates:
            return self._get_demo_businesses(city_query)
        
        try:
            # Search for businesses using NEW Places API
            places_url = "https://places.googleapis.com/v1/places:searchText"
            places_headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.api_key,
                'X-Goog-FieldMask': 'places.displayName,places.formattedAddress,places.rating,places.userRatingCount,places.location,places.types,places.nationalPhoneNumber,places.priceLevel,places.id,places.reviews'
            }
            
            # Define search queries for different business types
            search_queries = [
                f"restaurants in {city_query}",
                f"cafes in {city_query}",
                f"stores in {city_query}",
                f"services in {city_query}"
            ]
            
            for query in search_queries:
                places_data = {
                    "textQuery": query,
                    "maxResultCount": 5,
                    "locationBias": {
                        "circle": {
                            "center": {
                                "latitude": coordinates['lat'],
                                "longitude": coordinates['lng']
                            },
                            "radius": radius
                        }
                    }
                }
                
                response = requests.post(places_url, json=places_data, headers=places_headers)
                
                if response.status_code == 200:
                    result = response.json()
                    places = result.get('places', [])
                    
                    for place in places:
                        try:
                            business = self._parse_new_places_result(place, city_query)
                            if business:
                                businesses.append(business)
                        except Exception as e:
                            logging.warning(f"Error parsing place: {e}")
                            continue
                
                # Respect API rate limits
                time.sleep(0.2)
                
                if len(businesses) >= 15:  # Limit total results
                    break
            
            logging.info(f"Found {len(businesses)} businesses using NEW Places API")
            return businesses[:15]
            
        except Exception as e:
            logging.error(f"NEW Places API search failed: {e}")
            return self._get_demo_businesses(city_query)
    
    def _parse_new_places_result(self, place: Dict, city_query: str) -> Optional[BusinessResult]:
        """Parse a place result from NEW Places API"""
        try:
            # Extract city and region from query or address - handle international formats
            city_parts = city_query.split(',')
            city = city_parts[0].strip() if city_parts else "Unknown"
            
            # For international cities, use region instead of state
            region = "International"
            if len(city_parts) > 1:
                region = city_parts[1].strip()
            else:
                # Try to extract region from the formatted address
                address = place.get('formattedAddress', '')
                if address:
                    # Common patterns: "City, State, Country" or "City, Country"
                    addr_parts = address.split(', ')
                    if len(addr_parts) >= 2:
                        region = addr_parts[-1]  # Last part is usually country
                        if len(addr_parts) >= 3:
                            region = addr_parts[-2]  # Second to last might be state/province
            
            # Extract business info
            name = place.get('displayName', {}).get('text', 'Unknown Business')
            full_address = place.get('formattedAddress', 'No address')
            rating = place.get('rating', 4.0)
            review_count = place.get('userRatingCount', 0)
            location = place.get('location', {})
            latitude = location.get('latitude', 0.0)
            longitude = location.get('longitude', 0.0)
            place_id = place.get('id', '')
            phone = place.get('nationalPhoneNumber')
            price_level = place.get('priceLevel')
            
            # Determine category from types
            types = place.get('types', [])
            category, subcategory = self._categorize_business(types)
            
            return BusinessResult(
                name=name,
                address=full_address,
                city=city,
                state=region,
                rating=rating,
                review_count=review_count,
                category=category,
                subcategory=subcategory,
                phone=phone,
                latitude=latitude,
                longitude=longitude,
                place_id=place_id,
                price_level=self._convert_price_level(price_level)
            )
            
        except Exception as e:
            logging.error(f"Error parsing place result: {e}")
            return None
    
    def _categorize_business(self, types: List[str]) -> tuple:
        """Categorize business based on Google types"""
        type_mapping = {
            'restaurant': ('Restaurant', ['dining']),
            'food': ('Restaurant', ['dining']),
            'cafe': ('Cafe', ['coffee', 'beverages']),
            'store': ('Retail', ['shopping']),
            'clothing_store': ('Retail', ['clothing', 'fashion']),
            'gym': ('Fitness', ['health', 'exercise']),
            'beauty_salon': ('Beauty', ['personal care']),
            'bakery': ('Food', ['bakery', 'pastries']),
            'bar': ('Bar', ['drinks', 'nightlife']),
            'gas_station': ('Service', ['automotive']),
            'bank': ('Finance', ['banking']),
            'pharmacy': ('Health', ['medical', 'pharmacy'])
        }
        
        for type_name in types:
            if type_name in type_mapping:
                return type_mapping[type_name]
        
        # Default categorization
        if any(t in ['restaurant', 'food', 'meal_takeaway'] for t in types):
            return ('Restaurant', ['dining'])
        elif any(t in ['store', 'shopping'] for t in types):
            return ('Retail', ['shopping'])
        else:
            return ('Service', ['general'])
    
    def _convert_price_level(self, price_level) -> Optional[int]:
        """Convert Google price level to our format"""
        if price_level in ['PRICE_LEVEL_INEXPENSIVE', 'PRICE_LEVEL_FREE']:
            return 1
        elif price_level == 'PRICE_LEVEL_MODERATE':
            return 2
        elif price_level == 'PRICE_LEVEL_EXPENSIVE':
            return 3
        elif price_level == 'PRICE_LEVEL_VERY_EXPENSIVE':
            return 4
        return None
    
    def _get_demo_coordinates(self, city_query: str) -> Optional[Dict]:
        """Get demo coordinates for major cities worldwide"""
        demo_cities = {
            # Major US Cities
            'new york': {'lat': 40.7128, 'lng': -74.0060, 'formatted_address': 'New York, NY, USA'},
            'los angeles': {'lat': 34.0522, 'lng': -118.2437, 'formatted_address': 'Los Angeles, CA, USA'},
            'chicago': {'lat': 41.8781, 'lng': -87.6298, 'formatted_address': 'Chicago, IL, USA'},
            'houston': {'lat': 29.7604, 'lng': -95.3698, 'formatted_address': 'Houston, TX, USA'},
            'phoenix': {'lat': 33.4484, 'lng': -112.0740, 'formatted_address': 'Phoenix, AZ, USA'},
            'philadelphia': {'lat': 39.9526, 'lng': -75.1652, 'formatted_address': 'Philadelphia, PA, USA'},
            'san antonio': {'lat': 29.4241, 'lng': -98.4936, 'formatted_address': 'San Antonio, TX, USA'},
            'san diego': {'lat': 32.7157, 'lng': -117.1611, 'formatted_address': 'San Diego, CA, USA'},
            'dallas': {'lat': 32.7767, 'lng': -96.7970, 'formatted_address': 'Dallas, TX, USA'},
            'austin': {'lat': 30.2672, 'lng': -97.7431, 'formatted_address': 'Austin, TX, USA'},
            'seattle': {'lat': 47.6062, 'lng': -122.3321, 'formatted_address': 'Seattle, WA, USA'},
            'miami': {'lat': 25.7617, 'lng': -80.1918, 'formatted_address': 'Miami, FL, USA'},
            'atlanta': {'lat': 33.7490, 'lng': -84.3880, 'formatted_address': 'Atlanta, GA, USA'},
            'boston': {'lat': 42.3601, 'lng': -71.0589, 'formatted_address': 'Boston, MA, USA'},
            'denver': {'lat': 39.7392, 'lng': -104.9903, 'formatted_address': 'Denver, CO, USA'},
            'portland': {'lat': 45.5152, 'lng': -122.6784, 'formatted_address': 'Portland, OR, USA'},
            'las vegas': {'lat': 36.1699, 'lng': -115.1398, 'formatted_address': 'Las Vegas, NV, USA'},
            'nashville': {'lat': 36.1627, 'lng': -86.7816, 'formatted_address': 'Nashville, TN, USA'},
            'detroit': {'lat': 42.3314, 'lng': -83.0458, 'formatted_address': 'Detroit, MI, USA'},
            'memphis': {'lat': 35.1495, 'lng': -90.0490, 'formatted_address': 'Memphis, TN, USA'},
            'charlotte': {'lat': 35.2271, 'lng': -80.8431, 'formatted_address': 'Charlotte, NC, USA'},
            'tampa': {'lat': 27.9506, 'lng': -82.4572, 'formatted_address': 'Tampa, FL, USA'},
            'milwaukee': {'lat': 43.0389, 'lng': -87.9065, 'formatted_address': 'Milwaukee, WI, USA'},
            'oklahoma city': {'lat': 35.4676, 'lng': -97.5164, 'formatted_address': 'Oklahoma City, OK, USA'},
            'louisville': {'lat': 38.2527, 'lng': -85.7585, 'formatted_address': 'Louisville, KY, USA'},
            'baltimore': {'lat': 39.2904, 'lng': -76.6122, 'formatted_address': 'Baltimore, MD, USA'},
            'kansas city': {'lat': 39.0997, 'lng': -94.5786, 'formatted_address': 'Kansas City, MO, USA'},
            'virginia beach': {'lat': 36.8529, 'lng': -75.9780, 'formatted_address': 'Virginia Beach, VA, USA'},
            'omaha': {'lat': 41.2565, 'lng': -95.9345, 'formatted_address': 'Omaha, NE, USA'},
            'raleigh': {'lat': 35.7796, 'lng': -78.6382, 'formatted_address': 'Raleigh, NC, USA'},
            'colorado springs': {'lat': 38.8339, 'lng': -104.8214, 'formatted_address': 'Colorado Springs, CO, USA'},
            'tucson': {'lat': 32.2226, 'lng': -110.9747, 'formatted_address': 'Tucson, AZ, USA'},
            'fresno': {'lat': 36.7378, 'lng': -119.7871, 'formatted_address': 'Fresno, CA, USA'},
            'sacramento': {'lat': 38.5816, 'lng': -121.4944, 'formatted_address': 'Sacramento, CA, USA'},
            'mesa': {'lat': 33.4152, 'lng': -111.8315, 'formatted_address': 'Mesa, AZ, USA'},
            'arlington': {'lat': 32.7357, 'lng': -97.1081, 'formatted_address': 'Arlington, TX, USA'},
            'cleveland': {'lat': 41.4993, 'lng': -81.6944, 'formatted_address': 'Cleveland, OH, USA'},
            'minneapolis': {'lat': 44.9778, 'lng': -93.2650, 'formatted_address': 'Minneapolis, MN, USA'},
            'new orleans': {'lat': 29.9511, 'lng': -90.0715, 'formatted_address': 'New Orleans, LA, USA'},
            'pittsburgh': {'lat': 40.4406, 'lng': -79.9959, 'formatted_address': 'Pittsburgh, PA, USA'},
            'salt lake city': {'lat': 40.7608, 'lng': -111.8910, 'formatted_address': 'Salt Lake City, UT, USA'},
            'san francisco': {'lat': 37.7749, 'lng': -122.4194, 'formatted_address': 'San Francisco, CA, USA'},
            'san jose': {'lat': 37.3382, 'lng': -121.8863, 'formatted_address': 'San Jose, CA, USA'}
        }
        
        # Try to find the city by cleaning the input
        city_clean = city_query.lower().strip()
        
        # Remove common suffixes and try different variations
        for suffix in [', usa', ', us', ', canada', ', uk', ', england', ', france', ', germany', ', japan', ', australia']:
            if city_clean.endswith(suffix):
                city_clean = city_clean.replace(suffix, '').strip()
                break
        
        # Split by comma and take first part
        city_clean = city_clean.split(',')[0].strip()
        
        return demo_cities.get(city_clean)
    
    def _get_demo_businesses(self, city_query: str) -> List[BusinessResult]:
        """Generate demo businesses for a city (international support)"""
        city_clean = city_query.split(',')[0].strip()
        coordinates = self._get_demo_coordinates(city_query)
        
        if not coordinates:
            return []
        
        # Extract region/country from formatted address
        formatted_addr = coordinates.get('formatted_address', '')
        region = "International"
        if formatted_addr:
            parts = formatted_addr.split(', ')
            if len(parts) >= 2:
                region = parts[-1]  # Country/region is usually last
        
        # Generate culturally appropriate business names based on region
        business_types = self._get_regional_business_types(region)
        
        demo_businesses = []
        for i, (biz_type, category, rating_base) in enumerate(business_types):
            demo_businesses.append(BusinessResult(
                name=f"{city_clean} {biz_type}",
                address=f"{100 + i*10} {self._get_regional_street_name(region)}, {city_clean}",
                city=city_clean,
                state=region,
                rating=rating_base + random.uniform(-0.3, 0.5),
                review_count=random.randint(50, 300),
                category=category,
                subcategory=[category.lower()],
                phone=self._generate_regional_phone(region),
                latitude=coordinates['lat'] + random.uniform(-0.01, 0.01),
                longitude=coordinates['lng'] + random.uniform(-0.01, 0.01),
                place_id=f"demo_{city_clean}_{i}",
                price_level=random.randint(1, 3)
            ))
        
        return demo_businesses
    
    def _get_regional_business_types(self, region: str) -> List[tuple]:
        """Get US business types"""
        return [
            ("BBQ Joint", "Restaurant", 4.3),
            ("Coffee Shop", "Cafe", 4.4),
            ("Sports Bar", "Bar", 4.1),
            ("Fitness Center", "Fitness", 4.2),
            ("Auto Shop", "Service", 4.0),
            ("Pizza Place", "Restaurant", 4.2),
            ("Burger Joint", "Restaurant", 4.1),
            ("Steakhouse", "Restaurant", 4.4),
            ("Diner", "Restaurant", 4.2),
            ("Brewery", "Bar", 4.3),
            ("Nail Salon", "Service", 4.1),
            ("Gas Station", "Service", 3.9),
            ("Grocery Store", "Retail", 4.0),
            ("Hardware Store", "Retail", 4.1),
            ("Bookstore", "Retail", 4.3)
        ]
    
    def _get_regional_street_name(self, region: str) -> str:
        """Get US street names"""
        return random.choice([
            "Main Street", "Oak Avenue", "Elm Street", "Broadway", "First Street",
            "Park Avenue", "Washington Street", "Lincoln Avenue", "Church Street",
            "Market Street", "State Street", "Union Street", "Center Street"
        ])
    
    def _generate_regional_phone(self, region: str) -> str:
        """Generate US phone number format"""
        return f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"

    def get_business_reviews(self, place_id: str, max_reviews: int = 50) -> List[Dict]:
        """
        Fetch reviews for a specific business using place_id
        Enhanced to get maximum available review data
        """
        if not self.places_available:
            return self._generate_demo_reviews()
        
        try:
            # Get place details including comprehensive review data
            details_url = f"https://places.googleapis.com/v1/places/{place_id}"
            headers = {
                'X-Goog-Api-Key': self.api_key,
                'X-Goog-FieldMask': 'reviews.authorAttribution,reviews.publishTime,reviews.rating,reviews.text,reviews.originalText,reviews.relativePublishTimeDescription,displayName,rating,userRatingCount'
            }
            
            response = requests.get(details_url, headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                reviews_data = result.get('reviews', [])
                business_name = result.get('displayName', 'Unknown Business')
                business_rating = result.get('rating', 0)
                
                print(f"✅ Found {len(reviews_data)} reviews for {business_name}")
                
                reviews = []
                for review in reviews_data:
                    try:
                        # Extract author information
                        author_info = review.get('authorAttribution', {})
                        author_name = author_info.get('displayName', 'Anonymous')
                        author_uri = author_info.get('uri', '')
                        author_photo_uri = author_info.get('photoUri', '')
                        
                        # Extract review text (prefer original language)
                        text_obj = review.get('text', {})
                        original_text_obj = review.get('originalText', {})
                        
                        review_text = original_text_obj.get('text', '') or text_obj.get('text', '')
                        language_code = original_text_obj.get('languageCode') or text_obj.get('languageCode', 'en')
                        
                        # Extract timing information
                        publish_time = review.get('publishTime', '')
                        relative_time = review.get('relativePublishTimeDescription', '')
                        
                        processed_review = {
                            # Author information
                            'reviewer_name': author_name,
                            'author_name': author_name,  # Alias for compatibility
                            'author_url': author_uri,
                            'profile_photo_url': author_photo_uri,
                            
                            # Review content
                            'rating': review.get('rating', 0),
                            'review_text': review_text,
                            'text': review_text,  # Alias for compatibility
                            'language': language_code,
                            'original_language': language_code,
                            'translated': False,
                            
                            # Timing
                            'review_date': publish_time,
                            'time': publish_time,  # Alias for compatibility
                            'relative_time_description': relative_time,
                            
                            # Metadata
                            'source': 'google_places',
                            'place_id': place_id,
                            'business_name': business_name,
                            'business_rating': business_rating,
                            'helpful_votes': 0,  # Not available in Places API
                            'review_id': f"places_{place_id}_{hash(review_text + author_name + publish_time)}"
                        }
                        
                        # Enhanced sentiment analysis based on rating and text
                        if review_text:
                            rating_val = processed_review['rating']
                            
                            # Enhanced rating-based sentiment with text keywords
                            positive_keywords = ['great', 'excellent', 'amazing', 'wonderful', 'fantastic', 'love', 'best', 'perfect', 'awesome', 'outstanding']
                            negative_keywords = ['terrible', 'horrible', 'awful', 'hate', 'worst', 'bad', 'poor', 'disappointing', 'disgusting', 'pathetic']
                            
                            text_lower = review_text.lower()
                            positive_count = sum(1 for word in positive_keywords if word in text_lower)
                            negative_count = sum(1 for word in negative_keywords if word in text_lower)
                            
                            # Base sentiment from rating
                            if rating_val >= 4:
                                base_sentiment = 0.6 + (rating_val - 4) * 0.2
                                sentiment_label = 'positive'
                            elif rating_val >= 3:
                                base_sentiment = 0.0
                                sentiment_label = 'neutral'
                            else:
                                base_sentiment = -0.6 + (rating_val - 1) * 0.2
                                sentiment_label = 'negative'
                            
                            # Adjust based on text keywords
                            keyword_adjustment = (positive_count - negative_count) * 0.1
                            final_sentiment = max(-1.0, min(1.0, base_sentiment + keyword_adjustment))
                            
                            # Re-classify if keywords strongly indicate otherwise
                            if final_sentiment > 0.1:
                                sentiment_label = 'positive'
                            elif final_sentiment < -0.1:
                                sentiment_label = 'negative'
                            else:
                                sentiment_label = 'neutral'
                            
                            processed_review.update({
                                'sentiment_label': sentiment_label,
                                'sentiment_score': final_sentiment,
                                'sentiment_confidence': min(0.9, 0.5 + abs(final_sentiment) * 0.5),
                                'sentiment_method': 'rating_plus_keywords'
                            })
                            
                            # Extract keywords (improved)
                            import re
                            words = re.findall(r'\b[a-zA-Z]{3,}\b', text_lower)
                            # Filter out common stop words
                            stop_words = {'the', 'and', 'was', 'were', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how', 'its', 'may', 'new', 'now', 'old', 'see', 'two', 'way', 'who', 'boy', 'did', 'use', 'her', 'now', 'air', 'day', 'end', 'why'}
                            keywords = [word for word in words if word not in stop_words and len(word) > 3]
                            processed_review['keywords'] = list(set(keywords))[:15]  # Unique keywords, max 15
                        else:
                            # No text, sentiment based purely on rating
                            rating_val = processed_review['rating']
                            if rating_val >= 4:
                                sentiment_score = 0.5 + (rating_val - 4) * 0.25
                                sentiment_label = 'positive'
                            elif rating_val >= 3:
                                sentiment_score = 0.0
                                sentiment_label = 'neutral'
                            else:
                                sentiment_score = -0.5 + (rating_val - 1) * 0.25
                                sentiment_label = 'negative'
                            
                            processed_review.update({
                                'sentiment_label': sentiment_label,
                                'sentiment_score': sentiment_score,
                                'sentiment_confidence': 0.6,  # Lower confidence without text
                                'sentiment_method': 'rating_only',
                                'keywords': []
                            })
                        
                        reviews.append(processed_review)
                    except Exception as e:
                        logging.warning(f"Error processing review: {e}")
                        continue
                
                print(f"✅ Successfully processed {len(reviews)} reviews")
                return reviews[:max_reviews]  # Limit to max_reviews
            else:
                print(f"❌ API request failed with status {response.status_code}: {response.text}")
                return self._generate_demo_reviews()
            
        except Exception as e:
            logging.error(f"Error fetching reviews: {e}")
            return self._generate_demo_reviews()
        
        return self._generate_demo_reviews()
    
    def search_places_with_reviews(self, query: str, location: str = "", max_results: int = 20) -> List[Dict]:
        """
        Search for places and fetch their reviews in one operation
        Returns list of businesses with their reviews included
        """
        print(f"🔍 Searching for '{query}' in '{location}' with reviews...")
        
        try:
            # First, search for places
            businesses = self.search_businesses_near_city(f"{query} {location}", max_results=max_results)
            
            if not businesses:
                print("❌ No businesses found")
                return []
            
            print(f"✅ Found {len(businesses)} businesses, fetching reviews...")
            
            # For each business, fetch reviews
            enhanced_businesses = []
            for i, business in enumerate(businesses):
                try:
                    print(f"📖 Fetching reviews for {business.name} ({i+1}/{len(businesses)})")
                    
                    # Get reviews for this business
                    reviews = self.get_business_reviews(business.place_id, max_reviews=20)
                    
                    # Create enhanced business dict with reviews
                    enhanced_business = {
                        'name': business.name,
                        'address': business.address,
                        'city': business.city,
                        'state': business.state,
                        'rating': business.rating,
                        'review_count': business.review_count,
                        'category': business.category,
                        'subcategory': business.subcategory,
                        'phone': business.phone,
                        'latitude': business.latitude,
                        'longitude': business.longitude,
                        'place_id': business.place_id,
                        'price_level': business.price_level,
                        'reviews': reviews,
                        'reviews_fetched': len(reviews),
                        'avg_sentiment': sum(r.get('sentiment_score', 0) for r in reviews) / len(reviews) if reviews else 0,
                        'sentiment_distribution': self._calculate_sentiment_distribution(reviews)
                    }
                    
                    enhanced_businesses.append(enhanced_business)
                    
                    # Small delay to respect API limits
                    time.sleep(0.2)
                    
                except Exception as e:
                    print(f"⚠️ Error fetching reviews for {business.name}: {e}")
                    # Add business without reviews
                    enhanced_business = {
                        'name': business.name,
                        'address': business.address,
                        'city': business.city,
                        'state': business.state,
                        'rating': business.rating,
                        'review_count': business.review_count,
                        'category': business.category,
                        'subcategory': business.subcategory,
                        'phone': business.phone,
                        'latitude': business.latitude,
                        'longitude': business.longitude,
                        'place_id': business.place_id,
                        'price_level': business.price_level,
                        'reviews': [],
                        'reviews_fetched': 0,
                        'avg_sentiment': 0,
                        'sentiment_distribution': {'positive': 0, 'neutral': 0, 'negative': 0}
                    }
                    enhanced_businesses.append(enhanced_business)
                    continue
            
            print(f"🎉 Successfully enhanced {len(enhanced_businesses)} businesses with review data")
            return enhanced_businesses
            
        except Exception as e:
            print(f"❌ Error in search_places_with_reviews: {e}")
            return []
    
    def get_place_details_with_reviews(self, place_id: str) -> Optional[Dict]:
        """
        Get comprehensive place details including reviews
        Returns a complete business profile with all available data
        """
        try:
            # Get basic place details
            details_url = f"https://places.googleapis.com/v1/places/{place_id}"
            headers = {
                'X-Goog-Api-Key': self.api_key,
                'X-Goog-FieldMask': 'displayName,formattedAddress,rating,userRatingCount,location,types,nationalPhoneNumber,priceLevel,websiteUri,regularOpeningHours,reviews.authorAttribution,reviews.publishTime,reviews.rating,reviews.text,reviews.originalText,reviews.relativePublishTimeDescription'
            }
            
            response = requests.get(details_url, headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract location data
                location_data = result.get('location', {})
                lat = location_data.get('latitude', 0)
                lng = location_data.get('longitude', 0)
                
                # Process address
                address = result.get('formattedAddress', '')
                city, state = self._extract_city_state_from_address(address)
                
                # Process categories
                types = result.get('types', [])
                main_category = types[0] if types else 'business'
                subcategories = types[1:5] if len(types) > 1 else []
                
                # Process opening hours
                opening_hours = result.get('regularOpeningHours', {})
                hours_text = []
                if opening_hours.get('periods'):
                    # Convert periods to readable format
                    for period in opening_hours.get('periods', []):
                        if period.get('open') and period.get('close'):
                            open_time = period['open'].get('time', '')
                            close_time = period['close'].get('time', '')
                            day = period['open'].get('day', 0)
                            day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
                            if day < len(day_names):
                                hours_text.append(f"{day_names[day]}: {open_time} - {close_time}")
                
                # Get reviews using our enhanced method
                reviews = self.get_business_reviews(place_id, max_reviews=50)
                
                # Create comprehensive business profile
                business_profile = {
                    'place_id': place_id,
                    'name': result.get('displayName', 'Unknown Business'),
                    'address': address,
                    'city': city,
                    'state': state,
                    'latitude': lat,
                    'longitude': lng,
                    'rating': result.get('rating', 0),
                    'review_count': result.get('userRatingCount', 0),
                    'category': main_category,
                    'subcategory': subcategories,
                    'phone': result.get('nationalPhoneNumber', ''),
                    'website': result.get('websiteUri', ''),
                    'price_level': result.get('priceLevel'),
                    'opening_hours': hours_text,
                    'reviews': reviews,
                    'reviews_fetched': len(reviews),
                    'avg_sentiment': sum(r.get('sentiment_score', 0) for r in reviews) / len(reviews) if reviews else 0,
                    'sentiment_distribution': self._calculate_sentiment_distribution(reviews),
                    'keyword_analysis': self._extract_top_keywords(reviews),
                    'rating_distribution': self._calculate_rating_distribution(reviews)
                }
                
                print(f"✅ Fetched complete profile for {business_profile['name']} with {len(reviews)} reviews")
                return business_profile
                
            else:
                print(f"❌ Failed to fetch place details: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching place details: {e}")
            return None
    
    def _calculate_sentiment_distribution(self, reviews: List[Dict]) -> Dict[str, int]:
        """Calculate sentiment distribution from reviews"""
        distribution = {'positive': 0, 'neutral': 0, 'negative': 0}
        for review in reviews:
            sentiment = review.get('sentiment_label', 'neutral')
            if sentiment in distribution:
                distribution[sentiment] += 1
        return distribution
    
    def _extract_top_keywords(self, reviews: List[Dict], top_n: int = 20) -> List[Dict]:
        """Extract most common keywords from reviews"""
        from collections import Counter
        
        all_keywords = []
        for review in reviews:
            keywords = review.get('keywords', [])
            all_keywords.extend(keywords)
        
        if not all_keywords:
            return []
        
        # Count keyword frequency
        keyword_counts = Counter(all_keywords)
        
        # Return top keywords with their counts
        top_keywords = []
        for keyword, count in keyword_counts.most_common(top_n):
            top_keywords.append({
                'keyword': keyword,
                'count': count,
                'frequency': count / len(all_keywords) if all_keywords else 0
            })
        
        return top_keywords
    
    def _calculate_rating_distribution(self, reviews: List[Dict]) -> Dict[int, int]:
        """Calculate rating distribution from reviews"""
        distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for review in reviews:
            rating = review.get('rating', 0)
            if 1 <= rating <= 5:
                distribution[rating] += 1
        return distribution
    
    def _extract_city_state_from_address(self, address: str) -> tuple:
        """Extract city and state from formatted address"""
        try:
            if not address:
                return "Unknown City", "Unknown State"
            
            # Split by commas and get the parts
            parts = [part.strip() for part in address.split(',')]
            
            if len(parts) >= 3:
                # Format: "Street, City, State Zip, Country"
                city = parts[-3]
                state_zip = parts[-2]
                # Extract state from "State Zip" format
                state = state_zip.split()[0] if state_zip else "Unknown State"
                return city, state
            elif len(parts) == 2:
                # Format: "City, State" or "City, Country"
                city = parts[0]
                state = parts[1].split()[0] if parts[1] else "Unknown State"
                return city, state
            else:
                # Fallback
                return parts[0] if parts else "Unknown City", "Unknown State"
                
        except Exception as e:
            return "Unknown City", "Unknown State"

    def _generate_demo_reviews(self) -> List[Dict]:
        """Generate demo reviews when API is not available"""
        demo_reviews = [
            {
                'reviewer_name': 'John D.',
                'rating': 5,
                'review_text': 'Excellent service and great food! Highly recommend this place.',
                'review_date': '2024-10-15',
                'helpful_votes': 3,
                'source': 'demo',
                'sentiment_label': 'positive',
                'sentiment_score': 0.85,
                'keywords': ['excellent', 'service', 'great', 'food', 'recommend']
            },
            {
                'reviewer_name': 'Sarah M.',
                'rating': 4,
                'review_text': 'Good atmosphere and friendly staff. Will come back again.',
                'review_date': '2024-10-10',
                'helpful_votes': 2,
                'source': 'demo',
                'sentiment_label': 'positive',
                'sentiment_score': 0.75,
                'keywords': ['good', 'atmosphere', 'friendly', 'staff', 'back']
            },
            {
                'reviewer_name': 'Mike R.',
                'rating': 3,
                'review_text': 'Average experience. Nothing special but decent quality.',
                'review_date': '2024-10-05',
                'helpful_votes': 1,
                'source': 'demo',
                'sentiment_label': 'neutral',
                'sentiment_score': 0.5,
                'keywords': ['average', 'experience', 'decent', 'quality']
            }
        ]
        return demo_reviews


# For backward compatibility
LocationBusinessSearch = NewPlacesAPISearch