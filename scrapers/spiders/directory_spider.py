import scrapy
import json
import re
from datetime import datetime
from scrapers.items import BusinessItem, ReviewItem


class GooglePlacesSpider(scrapy.Spider):
    name = 'google_places'
    allowed_domains = ['google.com']
    
    def __init__(self, location='New York, NY', category='restaurant', *args, **kwargs):
        super(GooglePlacesSpider, self).__init__(*args, **kwargs)
        self.location = location
        self.category = category
        # Note: This is a simplified example. Google Places requires API access for production use.
        self.start_urls = [
            f'https://www.google.com/search?q={category}+near+{location.replace(" ", "+")}'
        ]

    def parse(self, response):
        # Extract business listings from Google search results
        # This is a basic example - for production, consider using Google Places API
        business_elements = response.css('[data-cid]')
        
        for element in business_elements[:10]:  # Limit results
            business = BusinessItem()
            
            business['name'] = element.css('h3::text').get()
            business['source'] = 'google'
            business['source_id'] = element.css('::attr(data-cid)').get()
            
            # Rating
            rating_text = element.css('[role="img"]::attr(aria-label)').get()
            if rating_text and 'star' in rating_text:
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                business['rating'] = float(rating_match.group(1)) if rating_match else None
            
            # Review count
            review_text = element.css('span:contains("reviews")::text').get()
            if review_text:
                review_match = re.search(r'(\d+)', review_text.replace(',', ''))
                business['review_count'] = int(review_match.group(1)) if review_match else 0
            
            # Address and other details would be extracted here
            # This is a simplified version
            
            if business['name']:
                yield business


class DirectorySpider(scrapy.Spider):
    name = 'directory'
    allowed_domains = ['yellowpages.com']
    
    def __init__(self, location='New York, NY', category='restaurants', *args, **kwargs):
        super(DirectorySpider, self).__init__(*args, **kwargs)
        self.location = location
        self.category = category
        self.start_urls = [
            f'https://www.yellowpages.com/search?search_terms={category}&geo_location_terms={location}'
        ]

    def parse(self, response):
        business_links = response.css('.result a.business-name::attr(href)').getall()
        
        for link in business_links[:15]:  # Limit results
            if link:
                yield response.follow(link, self.parse_business)
        
        # Follow pagination
        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)

    def parse_business(self, response):
        business = BusinessItem()
        
        business['name'] = response.css('h1::text').get()
        business['source'] = 'yellowpages'
        business['source_id'] = response.url.split('/')[-1]
        
        # Address
        address_parts = response.css('.address span::text').getall()
        business['address'] = ' '.join(address_parts) if address_parts else None
        
        # Phone
        business['phone'] = response.css('.phone::text').get()
        
        # Website
        business['website'] = response.css('a[title="Website"]::attr(href)').get()
        
        # Categories
        categories = response.css('.categories a::text').getall()
        business['category'] = categories[0] if categories else None
        
        # Hours
        hours_text = response.css('.hours-info::text').getall()
        if hours_text:
            business['hours'] = ' '.join(hours_text)
        
        # Description
        business['description'] = response.css('.description p::text').get()
        
        if business['name']:
            yield business