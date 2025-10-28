#!/usr/bin/env python3
"""
Location-based Business Search
Finds real businesses near user-specified cities using Google Places API
"""

import requests
import os
from typing import List, Dict, Optional
from datetime import datetime
import logging
from dataclasses import dataclass
import googlemaps

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

class LocationBusinessSearch:
    """Search for real businesses near a specified location"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('GOOGLE_PLACES_API_KEY')
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        
        # Initialize Google Maps client if API key is available
        if self.api_key:
            try:
                self.gmaps = googlemaps.Client(key=self.api_key)
                
                # Test the client with a simple request
                test_result = self.gmaps.geocode("New York")
                if test_result:
                    logging.info("Google Maps client initialized and tested successfully")
                else:
                    logging.warning("Google Maps client test failed - using demo mode")
                    self.gmaps = None
                    
            except Exception as e:
                logging.error(f"Google Maps client initialization failed: {e}")
                if "REQUEST_DENIED" in str(e):
                    logging.error("Please enable Geocoding API and Places API in Google Cloud Console")
                self.gmaps = None
                self.api_key = None
        else:
            self.gmaps = None
        
    def geocode_city(self, city_query: str) -> Optional[Dict]:
        """Get coordinates for a city name"""
        if not self.gmaps:
            logging.warning("Google Maps client not available. Using demo mode.")
            return self._get_demo_coordinates(city_query)
            
        try:
            # Use the official Google Maps geocoding
            geocode_result = self.gmaps.geocode(city_query)
            
            if geocode_result:
                location = geocode_result[0]
                return {
                    'lat': location['geometry']['location']['lat'],
                    'lng': location['geometry']['location']['lng'],
                    'formatted_address': location['formatted_address']
                }
        except Exception as e:
            logging.error(f"Geocoding error: {e}")
            
        return None
    
    def search_businesses_near_city(self, city_query: str, business_types: List[str] = None, radius: int = 5000) -> List[BusinessResult]:
        """Search for businesses near a city"""
        
        # Get city coordinates
        location = self.geocode_city(city_query)
        if not location:
            return []
        
        # If no Google Maps client, use demo mode
        if not self.gmaps:
            return self._get_demo_businesses(city_query)
        
        # Default business types if none specified
        if not business_types:
            business_types = [
                'restaurant', 'cafe', 'store', 'gym', 'spa', 'bakery',
                'book_store', 'clothing_store', 'electronics_store',
                'hair_care', 'dentist', 'lawyer', 'real_estate_agency'
            ]
        
        all_businesses = []
        
        for business_type in business_types[:5]:  # Limit to avoid API quotas
            businesses = self._search_places_by_type(
                location['lat'], 
                location['lng'], 
                business_type, 
                radius
            )
            all_businesses.extend(businesses)
            
            if len(all_businesses) >= 20:  # Limit total results
                break
        
        return all_businesses[:20]  # Return top 20
    
    def _search_places_by_type(self, lat: float, lng: float, place_type: str, radius: int) -> List[BusinessResult]:
        """Search for places of a specific type"""
        if not self.gmaps:
            return []
            
        try:
            # Use Google Maps Places Nearby Search
            places_result = self.gmaps.places_nearby(
                location=(lat, lng),
                radius=radius,
                type=place_type
            )
            
            businesses = []
            if places_result.get('status') == 'OK':
                for place in places_result['results'][:4]:  # Limit per type
                    business = self._parse_place_result(place)
                    if business:
                        businesses.append(business)
            
            return businesses
        except Exception as e:
            logging.error(f"Places search error: {e}")
            return []
    
    def _parse_place_result(self, place: Dict) -> Optional[BusinessResult]:
        """Parse Google Places API result into BusinessResult"""
        try:
            # Extract address components
            address_parts = place.get('vicinity', '').split(',')
            city = address_parts[-2].strip() if len(address_parts) >= 2 else ""
            state = address_parts[-1].strip() if len(address_parts) >= 1 else ""
            
            # Map Google place types to our categories
            category, subcategory = self._categorize_business(place.get('types', []))
            
            return BusinessResult(
                name=place['name'],
                address=place.get('vicinity', ''),
                city=city,
                state=state,
                rating=place.get('rating', 0.0),
                review_count=place.get('user_ratings_total', 0),
                category=category,
                subcategory=subcategory,
                phone=None,  # Would need additional API call
                latitude=place['geometry']['location']['lat'],
                longitude=place['geometry']['location']['lng'],
                place_id=place['place_id'],
                price_level=place.get('price_level')
            )
        except Exception as e:
            logging.error(f"Error parsing place result: {e}")
            return None
    
    def _categorize_business(self, types: List[str]) -> tuple:
        """Map Google place types to our business categories"""
        
        # Category mapping
        category_map = {
            'restaurant': ('Restaurant', ['Dining']),
            'food': ('Restaurant', ['Food']),
            'cafe': ('Cafe', ['Coffee']),
            'store': ('Retail', ['Shopping']),
            'clothing_store': ('Retail', ['Clothing']),
            'book_store': ('Retail', ['Books']),
            'electronics_store': ('Retail', ['Electronics']),
            'gym': ('Service', ['Fitness']),
            'spa': ('Service', ['Wellness']),
            'hair_care': ('Service', ['Beauty']),
            'dentist': ('Service', ['Healthcare']),
            'lawyer': ('Service', ['Legal']),
            'real_estate_agency': ('Service', ['Real Estate'])
        }
        
        for place_type in types:
            if place_type in category_map:
                return category_map[place_type]
        
        # Default categorization
        if any(t in types for t in ['restaurant', 'meal_takeaway', 'meal_delivery']):
            return ('Restaurant', ['Food'])
        elif any(t in types for t in ['store', 'shopping_mall']):
            return ('Retail', ['Shopping'])
        else:
            return ('Service', ['General'])
    
    def _get_demo_coordinates(self, city_query: str) -> Optional[Dict]:
        """Demo coordinates for major cities when API is unavailable"""
        demo_cities = {
            'new york': {'lat': 40.7128, 'lng': -74.0060, 'formatted_address': 'New York, NY, USA'},
            'nyc': {'lat': 40.7128, 'lng': -74.0060, 'formatted_address': 'New York, NY, USA'},
            'los angeles': {'lat': 34.0522, 'lng': -118.2437, 'formatted_address': 'Los Angeles, CA, USA'},
            'la': {'lat': 34.0522, 'lng': -118.2437, 'formatted_address': 'Los Angeles, CA, USA'},
            'chicago': {'lat': 41.8781, 'lng': -87.6298, 'formatted_address': 'Chicago, IL, USA'},
            'houston': {'lat': 29.7604, 'lng': -95.3698, 'formatted_address': 'Houston, TX, USA'},
            'phoenix': {'lat': 33.4484, 'lng': -112.0740, 'formatted_address': 'Phoenix, AZ, USA'},
            'philadelphia': {'lat': 39.9526, 'lng': -75.1652, 'formatted_address': 'Philadelphia, PA, USA'},
            'philly': {'lat': 39.9526, 'lng': -75.1652, 'formatted_address': 'Philadelphia, PA, USA'},
            'san antonio': {'lat': 29.4241, 'lng': -98.4936, 'formatted_address': 'San Antonio, TX, USA'},
            'san diego': {'lat': 32.7157, 'lng': -117.1611, 'formatted_address': 'San Diego, CA, USA'},
            'dallas': {'lat': 32.7767, 'lng': -96.7970, 'formatted_address': 'Dallas, TX, USA'},
            'san jose': {'lat': 37.3382, 'lng': -121.8863, 'formatted_address': 'San Jose, CA, USA'},
            'austin': {'lat': 30.2672, 'lng': -97.7431, 'formatted_address': 'Austin, TX, USA'},
            'jacksonville': {'lat': 30.3322, 'lng': -81.6557, 'formatted_address': 'Jacksonville, FL, USA'},
            'san francisco': {'lat': 37.7749, 'lng': -122.4194, 'formatted_address': 'San Francisco, CA, USA'},
            'sf': {'lat': 37.7749, 'lng': -122.4194, 'formatted_address': 'San Francisco, CA, USA'},
            'columbus': {'lat': 39.9612, 'lng': -82.9988, 'formatted_address': 'Columbus, OH, USA'},
            'charlotte': {'lat': 35.2271, 'lng': -80.8431, 'formatted_address': 'Charlotte, NC, USA'},
            'fort worth': {'lat': 32.7555, 'lng': -97.3308, 'formatted_address': 'Fort Worth, TX, USA'},
            'detroit': {'lat': 42.3314, 'lng': -83.0458, 'formatted_address': 'Detroit, MI, USA'},
            'el paso': {'lat': 31.7619, 'lng': -106.4850, 'formatted_address': 'El Paso, TX, USA'},
            'memphis': {'lat': 35.1495, 'lng': -90.0490, 'formatted_address': 'Memphis, TN, USA'},
            'seattle': {'lat': 47.6062, 'lng': -122.3321, 'formatted_address': 'Seattle, WA, USA'},
            'denver': {'lat': 39.7392, 'lng': -104.9903, 'formatted_address': 'Denver, CO, USA'},
            'washington': {'lat': 38.9072, 'lng': -77.0369, 'formatted_address': 'Washington, DC, USA'},
            'dc': {'lat': 38.9072, 'lng': -77.0369, 'formatted_address': 'Washington, DC, USA'},
            'boston': {'lat': 42.3601, 'lng': -71.0589, 'formatted_address': 'Boston, MA, USA'},
            'nashville': {'lat': 36.1627, 'lng': -86.7816, 'formatted_address': 'Nashville, TN, USA'},
            'baltimore': {'lat': 39.2904, 'lng': -76.6122, 'formatted_address': 'Baltimore, MD, USA'},
            'oklahoma city': {'lat': 35.4676, 'lng': -97.5164, 'formatted_address': 'Oklahoma City, OK, USA'},
            'portland': {'lat': 45.5152, 'lng': -122.6784, 'formatted_address': 'Portland, OR, USA'},
            'las vegas': {'lat': 36.1699, 'lng': -115.1398, 'formatted_address': 'Las Vegas, NV, USA'},
            'milwaukee': {'lat': 43.0389, 'lng': -87.9065, 'formatted_address': 'Milwaukee, WI, USA'},
            'albuquerque': {'lat': 35.0844, 'lng': -106.6504, 'formatted_address': 'Albuquerque, NM, USA'},
            'tucson': {'lat': 32.2226, 'lng': -110.9747, 'formatted_address': 'Tucson, AZ, USA'},
            'fresno': {'lat': 36.7378, 'lng': -119.7871, 'formatted_address': 'Fresno, CA, USA'},
            'sacramento': {'lat': 38.5816, 'lng': -121.4944, 'formatted_address': 'Sacramento, CA, USA'},
            'kansas city': {'lat': 39.0997, 'lng': -94.5786, 'formatted_address': 'Kansas City, MO, USA'},
            'mesa': {'lat': 33.4152, 'lng': -111.8315, 'formatted_address': 'Mesa, AZ, USA'},
            'atlanta': {'lat': 33.7490, 'lng': -84.3880, 'formatted_address': 'Atlanta, GA, USA'},
            'colorado springs': {'lat': 38.8339, 'lng': -104.8214, 'formatted_address': 'Colorado Springs, CO, USA'},
            'omaha': {'lat': 41.2565, 'lng': -95.9345, 'formatted_address': 'Omaha, NE, USA'},
            'raleigh': {'lat': 35.7796, 'lng': -78.6382, 'formatted_address': 'Raleigh, NC, USA'},
            'miami': {'lat': 25.7617, 'lng': -80.1918, 'formatted_address': 'Miami, FL, USA'},
            'cleveland': {'lat': 41.4993, 'lng': -81.6944, 'formatted_address': 'Cleveland, OH, USA'},
            'tulsa': {'lat': 36.1540, 'lng': -95.9928, 'formatted_address': 'Tulsa, OK, USA'},
            'oakland': {'lat': 37.8044, 'lng': -122.2712, 'formatted_address': 'Oakland, CA, USA'},
            'minneapolis': {'lat': 44.9778, 'lng': -93.2650, 'formatted_address': 'Minneapolis, MN, USA'},
            'wichita': {'lat': 37.6872, 'lng': -97.3301, 'formatted_address': 'Wichita, KS, USA'},
            'arlington': {'lat': 32.7357, 'lng': -97.1081, 'formatted_address': 'Arlington, TX, USA'}
        }
        
        city_lower = city_query.lower()
        for city_key in demo_cities:
            if city_key in city_lower or city_lower in city_key:
                return demo_cities[city_key]
        
        return None
    
    def _get_demo_businesses(self, city_query: str) -> List[BusinessResult]:
        """Generate demo businesses when API is unavailable"""
        location = self._get_demo_coordinates(city_query)
        if not location:
            return []
        
        # Extract city and state from formatted address
        parts = location['formatted_address'].split(',')
        city = parts[0].strip()
        state = parts[1].strip() if len(parts) > 1 else ""
        
        demo_businesses = [
            BusinessResult(
                name=f"{city} Coffee House",
                address=f"123 Main Street, {city}, {state}",
                city=city,
                state=state,
                rating=4.3,
                review_count=89,
                category="Cafe",
                subcategory=["Coffee", "Breakfast"],
                phone="(555) 123-4567",
                latitude=location['lat'] + 0.001,
                longitude=location['lng'] + 0.001,
                place_id=f"demo_{city.lower().replace(' ', '_')}_cafe",
                price_level=2
            ),
            BusinessResult(
                name=f"Local {city} Diner",
                address=f"456 Oak Avenue, {city}, {state}",
                city=city,
                state=state,
                rating=4.1,
                review_count=156,
                category="Restaurant",
                subcategory=["American", "Diner"],
                phone="(555) 234-5678",
                latitude=location['lat'] - 0.002,
                longitude=location['lng'] + 0.003,
                place_id=f"demo_{city.lower().replace(' ', '_')}_diner",
                price_level=2
            ),
            BusinessResult(
                name=f"{city} Fitness Center",
                address=f"789 Elm Street, {city}, {state}",
                city=city,
                state=state,
                rating=4.5,
                review_count=203,
                category="Service",
                subcategory=["Fitness", "Health"],
                phone="(555) 345-6789",
                latitude=location['lat'] + 0.003,
                longitude=location['lng'] - 0.001,
                place_id=f"demo_{city.lower().replace(' ', '_')}_gym",
                price_level=3
            )
        ]
        
        return demo_businesses

if __name__ == "__main__":
    # Test the location search
    searcher = LocationBusinessSearch()
    
    test_cities = ["Chicago, IL", "Austin, TX", "Portland, OR"]
    
    for city in test_cities:
        print(f"\n=== Searching businesses in {city} ===")
        businesses = searcher.search_businesses_near_city(city)
        
        for business in businesses:
            print(f"🏪 {business.name}")
            print(f"   📍 {business.address}")
            print(f"   ⭐ {business.rating}/5 ({business.review_count} reviews)")
            print(f"   🏷️ {business.category} - {', '.join(business.subcategory)}")