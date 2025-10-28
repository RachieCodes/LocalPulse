import scrapy
from scrapy import Item, Field


class BusinessItem(Item):
    name = Field()
    address = Field()
    phone = Field()
    website = Field()
    category = Field()
    subcategory = Field()
    rating = Field()
    review_count = Field()
    price_range = Field()
    hours = Field()
    latitude = Field()
    longitude = Field()
    description = Field()
    images = Field()
    amenities = Field()
    source = Field()
    source_id = Field()
    last_updated = Field()


class ReviewItem(Item):
    business_id = Field()
    business_name = Field()
    reviewer_name = Field()
    reviewer_id = Field()
    rating = Field()
    review_text = Field()
    review_date = Field()
    helpful_votes = Field()
    source = Field()
    source_review_id = Field()
    last_updated = Field()


class EventItem(Item):
    name = Field()
    description = Field()
    venue = Field()
    venue_address = Field()
    date = Field()
    time = Field()
    price = Field()
    category = Field()
    organizer = Field()
    latitude = Field()
    longitude = Field()
    source = Field()
    source_id = Field()
    last_updated = Field()