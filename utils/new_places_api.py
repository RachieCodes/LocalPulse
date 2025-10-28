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

    def get_business_reviews(self, place_id: str) -> List[Dict]:
        """Fetch reviews for a specific business using place_id"""
        if not self.places_available:
            return self._generate_demo_reviews()
        
        try:
            # Get place details including reviews
            details_url = f"https://places.googleapis.com/v1/places/{place_id}"
            headers = {
                'X-Goog-Api-Key': self.api_key,
                'X-Goog-FieldMask': 'reviews,displayName'
            }
            
            response = requests.get(details_url, headers=headers)
            
            if response.status_code == 200:
                result = response.json()
                reviews_data = result.get('reviews', [])
                
                reviews = []
                for review in reviews_data:
                    try:
                        processed_review = {
                            'reviewer_name': review.get('authorAttribution', {}).get('displayName', 'Anonymous'),
                            'rating': review.get('rating', 0),
                            'review_text': review.get('text', {}).get('text', ''),
                            'review_date': review.get('publishTime', ''),
                            'helpful_votes': 0,  # Not available in Places API
                            'source': 'google_places',
                            'place_id': place_id
                        }
                        
                        # Basic sentiment analysis
                        text = processed_review['review_text']
                        if text:
                            # Simple sentiment scoring based on rating
                            rating = processed_review['rating']
                            if rating >= 4:
                                processed_review['sentiment_label'] = 'positive'
                                processed_review['sentiment_score'] = 0.7 + (rating - 4) * 0.15
                            elif rating >= 3:
                                processed_review['sentiment_label'] = 'neutral'
                                processed_review['sentiment_score'] = 0.4 + (rating - 3) * 0.2
                            else:
                                processed_review['sentiment_label'] = 'negative'
                                processed_review['sentiment_score'] = rating * 0.2
                            
                            # Extract basic keywords (simple approach)
                            words = text.lower().split()
                            keywords = [word for word in words if len(word) > 3 and word.isalpha()]
                            processed_review['keywords'] = keywords[:10]  # Top 10 keywords
                        
                        reviews.append(processed_review)
                    except Exception as e:
                        logging.warning(f"Error processing review: {e}")
                        continue
                
                return reviews
            
        except Exception as e:
            logging.error(f"Error fetching reviews: {e}")
            return self._generate_demo_reviews()
        
        return self._generate_demo_reviews()
    
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