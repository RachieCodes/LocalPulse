import scrapy
import json
import re
from datetime import datetime
from scrapers.items import BusinessItem, ReviewItem


class YelpSpider(scrapy.Spider):
    name = 'yelp'
    allowed_domains = ['yelp.com']
    
    def __init__(self, location='New York, NY', category='restaurants', *args, **kwargs):
        super(YelpSpider, self).__init__(*args, **kwargs)
        self.location = location
        self.category = category
        self.start_urls = [
            f'https://www.yelp.com/search?find_desc={category}&find_loc={location}'
        ]

    def parse(self, response):
        # Extract business links from search results
        business_links = response.css('a[href*="/biz/"]::attr(href)').getall()
        
        for link in business_links[:20]:  # Limit to first 20 results
            if link.startswith('/biz/'):
                yield response.follow(link, self.parse_business)
        
        # Follow pagination
        next_page = response.css('a[aria-label="Next"]::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)

    def parse_business(self, response):
        # Extract business information
        business = BusinessItem()
        
        business['name'] = response.css('h1::text').get()
        business['source'] = 'yelp'
        business['source_id'] = response.url.split('/')[-1].split('?')[0]
        
        # Rating and review count
        rating_text = response.css('[data-testid="rating"] span::attr(aria-label)').get()
        if rating_text:
            rating_match = re.search(r'(\d+\.?\d*) star', rating_text)
            business['rating'] = float(rating_match.group(1)) if rating_match else None
            
        review_count_text = response.css('a[href*="reviews"] span::text').get()
        if review_count_text:
            review_match = re.search(r'(\d+)', review_count_text.replace(',', ''))
            business['review_count'] = int(review_match.group(1)) if review_match else 0
        
        # Address
        address_parts = response.css('[data-testid="business-address"] p::text').getall()
        business['address'] = ' '.join(address_parts) if address_parts else None
        
        # Phone
        business['phone'] = response.css('[data-testid="business-phone"] p::text').get()
        
        # Website
        business['website'] = response.css('a[href*="biz_redir"]::attr(href)').get()
        
        # Categories
        categories = response.css('[data-testid="business-categories"] a::text').getall()
        business['category'] = categories[0] if categories else None
        business['subcategory'] = categories[1:] if len(categories) > 1 else None
        
        # Price range
        price_range = response.css('[data-testid="business-price"] span::text').get()
        business['price_range'] = price_range
        
        # Hours
        hours_elements = response.css('[data-testid="business-hours"] tr')
        hours = {}
        for hour_element in hours_elements:
            day = hour_element.css('th::text').get()
            time = hour_element.css('td p::text').get()
            if day and time:
                hours[day] = time
        business['hours'] = hours
        
        # Description
        business['description'] = response.css('[data-testid="business-description"] p::text').get()
        
        # Images
        images = response.css('.photo-box img::attr(src)').getall()
        business['images'] = images[:5]  # Limit to first 5 images
        
        # Try to extract coordinates from script tags
        script_content = response.css('script::text').getall()
        for script in script_content:
            if 'mapMarkerProps' in script:
                lat_match = re.search(r'"latitude":(\d+\.?\d*)', script)
                lng_match = re.search(r'"longitude":(-?\d+\.?\d*)', script)
                if lat_match and lng_match:
                    business['latitude'] = float(lat_match.group(1))
                    business['longitude'] = float(lng_match.group(1))
                    break
        
        yield business
        
        # Extract reviews
        reviews_url = response.url + '?tab=reviews'
        yield response.follow(reviews_url, self.parse_reviews, 
                            meta={'business_id': business['source_id'], 
                                  'business_name': business['name']})

    def parse_reviews(self, response):
        business_id = response.meta['business_id']
        business_name = response.meta['business_name']
        
        review_elements = response.css('[data-testid="reviews-list"] > div')
        
        for review_element in review_elements:
            review = ReviewItem()
            review['business_id'] = business_id
            review['business_name'] = business_name
            review['source'] = 'yelp'
            
            # Reviewer info
            review['reviewer_name'] = review_element.css('.user-name a::text').get()
            review['reviewer_id'] = review_element.css('.user-name a::attr(href)').get()
            
            # Rating
            rating_div = review_element.css('[role="img"]::attr(aria-label)').get()
            if rating_div:
                rating_match = re.search(r'(\d+) star', rating_div)
                review['rating'] = int(rating_match.group(1)) if rating_match else None
            
            # Review text
            review['review_text'] = review_element.css('.comment p::text').get()
            
            # Date
            date_text = review_element.css('.review-date::text').get()
            if date_text:
                try:
                    review['review_date'] = datetime.strptime(date_text, '%m/%d/%Y')
                except:
                    review['review_date'] = None
            
            # Helpful votes
            helpful_text = review_element.css('.helpful-count::text').get()
            if helpful_text:
                helpful_match = re.search(r'(\d+)', helpful_text)
                review['helpful_votes'] = int(helpful_match.group(1)) if helpful_match else 0
            
            review['source_review_id'] = f"{business_id}_{review['reviewer_id']}"
            
            if review['review_text']:  # Only yield if there's actual review text
                yield review