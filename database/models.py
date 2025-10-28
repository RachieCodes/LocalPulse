from datetime import datetime
from typing import Optional, Dict, List, Any
from dataclasses import dataclass


@dataclass
class Business:
    """Business data model"""
    name: str
    address: str
    source: str
    source_id: str
    phone: Optional[str] = None
    website: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[List[str]] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    price_range: Optional[str] = None
    hours: Optional[Dict[str, str]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: Optional[str] = None
    images: Optional[List[str]] = None
    amenities: Optional[List[str]] = None
    last_updated: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        data = {
            'name': self.name,
            'address': self.address,
            'source': self.source,
            'source_id': self.source_id,
            'phone': self.phone,
            'website': self.website,
            'category': self.category,
            'subcategory': self.subcategory,
            'rating': self.rating,
            'review_count': self.review_count,
            'price_range': self.price_range,
            'hours': self.hours,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'description': self.description,
            'images': self.images,
            'amenities': self.amenities,
            'last_updated': self.last_updated or datetime.now()
        }
        
        # Add geospatial location field
        if self.latitude and self.longitude:
            data['location'] = {
                'type': 'Point',
                'coordinates': [self.longitude, self.latitude]
            }
        
        return data


@dataclass
class Review:
    """Review data model"""
    business_id: str
    business_name: str
    source: str
    source_review_id: str
    reviewer_name: Optional[str] = None
    reviewer_id: Optional[str] = None
    rating: Optional[int] = None
    review_text: Optional[str] = None
    review_date: Optional[datetime] = None
    helpful_votes: Optional[int] = None
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[str] = None
    keywords: Optional[List[str]] = None
    last_updated: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        return {
            'business_id': self.business_id,
            'business_name': self.business_name,
            'source': self.source,
            'source_review_id': self.source_review_id,
            'reviewer_name': self.reviewer_name,
            'reviewer_id': self.reviewer_id,
            'rating': self.rating,
            'review_text': self.review_text,
            'review_date': self.review_date,
            'helpful_votes': self.helpful_votes,
            'sentiment_score': self.sentiment_score,
            'sentiment_label': self.sentiment_label,
            'keywords': self.keywords,
            'last_updated': self.last_updated or datetime.now()
        }


@dataclass
class Event:
    """Event data model"""
    name: str
    source: str
    source_id: str
    description: Optional[str] = None
    venue: Optional[str] = None
    venue_address: Optional[str] = None
    date: Optional[datetime] = None
    time: Optional[str] = None
    price: Optional[str] = None
    category: Optional[str] = None
    organizer: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    last_updated: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        data = {
            'name': self.name,
            'source': self.source,
            'source_id': self.source_id,
            'description': self.description,
            'venue': self.venue,
            'venue_address': self.venue_address,
            'date': self.date,
            'time': self.time,
            'price': self.price,
            'category': self.category,
            'organizer': self.organizer,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'last_updated': self.last_updated or datetime.now()
        }
        
        # Add geospatial location field
        if self.latitude and self.longitude:
            data['location'] = {
                'type': 'Point',
                'coordinates': [self.longitude, self.latitude]
            }
        
        return data


@dataclass
class BusinessAnalytics:
    """Analytics data for a business"""
    business_id: str
    avg_rating: float
    total_reviews: int
    avg_sentiment: float
    latest_review: datetime
    earliest_review: datetime
    rating_distribution: Dict[int, int]
    sentiment_trend: List[Dict[str, Any]]
    top_keywords: List[Dict[str, Any]]
    monthly_review_counts: Dict[str, int]