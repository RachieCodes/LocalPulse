# LocalPulse - Local Business Analytics Dashboard

A comprehensive dashboard for analyzing local businesses using enhanced Google Places API, web scraping, and advanced sentiment analysis.

## 🌟 Features

### Dashboard Capabilities
- **Enhanced Google Places Reviews**: Real-time review fetching using `places/{placeId}/reviews` pattern
- **Advanced sentiment analysis** with multi-method approach (rating + keyword-based scoring)
- **Top-rated businesses** by category and location with comprehensive review data
- **Interactive review search** - find businesses and fetch their reviews in one operation
- **Keyword cloud** from reviews with trending insights and frequency analysis
- **Opening hours vs. peak review times** analysis
- **Competitor comparison** with market benchmarks
- **Geospatial visualization** on interactive maps
- **Rating anomaly detection** (sudden drops/increases)

### Enhanced Google Places API Integration
- **Real-time review fetching**: Uses Google Places API (New) for comprehensive review data
- **Enhanced data extraction**: Author info, review text, ratings, timestamps, relative time descriptions
- **Smart sentiment analysis**: Combines rating-based scoring with keyword detection
- **Automatic keyword extraction**: Advanced text processing with stop-word filtering
- **Confidence scoring**: Provides confidence levels for sentiment predictions
- **Batch processing**: Efficiently handles multiple businesses with rate limiting

### Technical Stack
- **Enhanced Google Places API**: Real-time business and review data
- **Scrapy**: Web scraping for Yelp, Google Places, business directories
- **MongoDB**: Structured data storage with geospatial indexing
- **Streamlit**: Interactive dashboard framework
- **Advanced NLP**: Multi-method sentiment analysis and keyword extraction
- **Celery + Redis**: Automated scheduling and background tasks
- **Plotly**: Advanced data visualizations

## 📁 Project Structure

```
LocalPulse/
├── scrapers/               # Scrapy spiders for data collection
│   ├── spiders/
│   │   ├── yelp_spider.py
│   │   ├── directory_spider.py
│   │   └── google_places_spider.py
│   ├── items.py           # Data models for scraped items
│   ├── pipelines.py       # Data processing pipelines
│   └── settings.py        # Scrapy configuration
├── database/              # MongoDB models and utilities
│   ├── mongo_client.py    # Database connection and queries
│   └── models.py          # Data models (Business, Review, Event)
├── utils/                 # Utility functions
│   ├── nlp_processor.py   # Sentiment analysis and keyword extraction
│   └── data_pipeline.py   # Data processing workflows
├── dashboard/             # Streamlit dashboard
│   └── main_dashboard.py  # Main dashboard application
├── scheduler/             # Celery task scheduler
│   ├── celery_app.py      # Celery configuration
│   ├── tasks.py           # Background tasks
│   └── manager.py         # Scheduler management utilities
├── requirements.txt       # Python dependencies
├── setup.py              # Automated setup script
└── README.md             # This file
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- MongoDB Community Edition
- Redis server

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/RachieCodes/LocalPulse.git
   cd LocalPulse
   ```

2. **Run the setup script**
   ```bash
   python setup.py
   ```
   
   This will:
   - Install Python dependencies
   - Create necessary directories
   - Download NLTK data
   - Setup environment configuration
   - Test database connections

3. **Start the services**
   
   **Start MongoDB** (in separate terminal):
   ```bash
   mongod
   ```
   
   **Start Redis** (in separate terminal):
   ```bash
   redis-server
   ```
   
   **Start the dashboard** (in separate terminal):
   ```bash
   python start_dashboard.py
   ```
   
   **Start the scheduler** (in separate terminal):
   ```bash
   python start_scheduler.py start
   ```

4. **Access the dashboard**
   
   Open your browser and go to: http://localhost:8501

## 📊 Dashboard Features

### 🏆 Top Businesses Tab
- Business rankings by rating and review count
- Category distribution charts
- Interactive map with business locations
- Filtering by category and location

### 📈 Sentiment Analysis Tab
- Sentiment distribution (positive/negative/neutral)
- Sentiment trends over time
- Most positive and negative reviews
- Correlation with ratings

### ☁️ Keyword Insights Tab
- Interactive word cloud
- Keyword frequency analysis
- Sentiment-keyword correlations
- Trending keywords identification

### ⏰ Time Analytics Tab
- Review volume over time
- Peak review times vs business hours
- Seasonal trends analysis
- Day-of-week patterns

### 🔍 Competitor Analysis Tab
- Side-by-side business comparisons
- Market average benchmarks
- Performance metrics visualization
- Category-based analysis

## 🌐 Enhanced Google Places API

### Real-time Review Fetching
The enhanced Google Places API integration provides comprehensive review data using the `places/{placeId}/reviews` pattern:

```python
from utils.new_places_api import NewPlacesAPISearch

# Initialize API
api = NewPlacesAPISearch()

# Method 1: Search places and fetch reviews in one operation
results = api.search_places_with_reviews(
    query="coffee shops",
    location="San Francisco, CA", 
    max_results=20
)

# Method 2: Get comprehensive place details with reviews
place_details = api.get_place_details_with_reviews(place_id)

# Method 3: Fetch reviews for specific place
reviews = api.get_business_reviews(place_id, max_reviews=50)
```

### Enhanced Data Features
- **Comprehensive Review Data**: Author info, text, ratings, timestamps, relative time
- **Smart Sentiment Analysis**: Rating + keyword-based scoring with confidence levels
- **Automatic Keyword Extraction**: Advanced text processing with stop-word filtering
- **Batch Processing**: Efficient handling of multiple businesses with rate limiting
- **Dashboard Integration**: Seamless integration with existing analytics

### Dashboard Integration
- **⭐ Fetch Reviews for Existing Businesses**: Automatically fetch reviews for businesses already in database
- **🔍 Search Places with Reviews**: Find new businesses and fetch their reviews simultaneously
- **📊 Enhanced Analytics**: Multi-method sentiment analysis with confidence scoring
- **🎯 Smart Processing**: Automatic duplicate detection and data normalization

### API Setup
1. **Get Google Places API Key**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Enable "Places API (New)"
   - Create an API key
   - Add to `.env`: `GOOGLE_PLACES_API_KEY=your_api_key_here`

2. **Test Integration**:
   ```bash
   python example_enhanced_places_api.py
   ```

## 🕷️ Data Collection

### Supported Sources
- **Yelp**: Business info, reviews, ratings, photos
- **Yellow Pages**: Business directories and contact info
- **Google Places**: Location data and basic reviews

### Running Scrapers Manually

**Scrape Yelp data**:
```bash
cd scrapers
scrapy crawl yelp -a category=restaurants -a location="New York, NY"
```

**Scrape directory data**:
```bash
cd scrapers
scrapy crawl directory -a category=restaurants -a location="New York, NY"
```

### Automated Scheduling

The scheduler runs the following tasks automatically:
- **Daily**: Yelp business scraping
- **Weekly**: Directory scraping  
- **Hourly**: Review sentiment processing
- **Every 6 hours**: Trending keyword generation
- **Daily**: Rating anomaly detection

## 🔧 Configuration

### Environment Variables (.env)
```bash
# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=localpulse

# Redis
REDIS_URL=redis://localhost:6379/0

# Dashboard
STREAMLIT_SERVER_PORT=8501
```

### Scrapy Settings (scrapers/settings.py)
```python
# Adjust scraping behavior
DOWNLOAD_DELAY = 3          # Delay between requests
RANDOMIZE_DOWNLOAD_DELAY = 0.5
CONCURRENT_REQUESTS = 16
ROBOTSTXT_OBEY = True
```

## 📈 Advanced Usage

### Custom Task Execution
```bash
# Process new reviews immediately
python start_scheduler.py task --task-name process_new_reviews --task-args 100

# Update business analytics
python start_scheduler.py task --task-name update_business_analytics

# Generate trending keywords
python start_scheduler.py task --task-name generate_trending_keywords --task-args 7
```

### Monitoring
```bash
# Start Flower for task monitoring
python start_scheduler.py flower

# Check service status
python start_scheduler.py status
```

### Database Queries
```python
from database.mongo_client import MongoDatabase

db = MongoDatabase()
db.connect()

# Get top restaurants
restaurants = db.get_businesses_by_category("restaurant", limit=20)

# Get reviews for a business
reviews = db.get_reviews_for_business("business_id")

# Get businesses near location
nearby = db.get_businesses_near_location(-74.0060, 40.7128, max_distance=1000)
```

## 🛠️ Development

### Adding New Scrapers
1. Create new spider in `scrapers/spiders/`
2. Define data extraction logic
3. Update `items.py` if needed
4. Add to scheduler tasks

### Extending Dashboard
1. Add new tab in `dashboard/main_dashboard.py`
2. Create visualization functions
3. Add data processing utilities in `utils/`

### Custom NLP Processing
1. Extend `utils/nlp_processor.py`
2. Add new sentiment analysis models
3. Implement custom keyword extraction

## 📋 API Documentation

### Database Models

**Business Model**:
```python
{
    "name": str,
    "address": str,
    "category": str,
    "rating": float,
    "review_count": int,
    "latitude": float,
    "longitude": float,
    "hours": dict,
    "source": str
}
```

**Review Model**:
```python
{
    "business_id": str,
    "rating": int,
    "review_text": str,
    "review_date": datetime,
    "sentiment_score": float,
    "sentiment_label": str,
    "keywords": list
}
```

## 🔍 Troubleshooting

### Common Issues

**MongoDB Connection Failed**:
- Ensure MongoDB is running: `mongod`
- Check connection string in `.env`

**Redis Connection Failed**:
- Start Redis server: `redis-server`
- Verify Redis is accessible: `redis-cli ping`

**Scraping Errors**:
- Check robots.txt compliance
- Reduce scraping speed in settings
- Verify target website structure

**Dashboard Not Loading**:
- Check port 8501 is available
- Verify all dependencies installed
- Check console for error messages

### Performance Optimization

**For Large Datasets**:
- Increase MongoDB indexes
- Adjust Celery worker concurrency
- Implement data pagination
- Use caching for frequent queries

**For Faster Scraping**:
- Adjust DOWNLOAD_DELAY
- Increase CONCURRENT_REQUESTS
- Use proxy rotation
- Implement request caching

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 🐛 Support

For issues and questions:
1. Check the troubleshooting section
2. Search existing issues on GitHub
3. Create a new issue with detailed information

## 🎯 Roadmap

### Upcoming Features
- [ ] Machine learning for demand forecasting
- [ ] Social media integration (Twitter, Instagram)
- [ ] Mobile app companion
- [ ] Advanced geospatial analysis
- [ ] Real-time alerts and notifications
- [ ] Export functionality (PDF reports)
- [ ] Multi-language support
- [ ] API endpoints for third-party integrations

### Performance Improvements
- [ ] Database query optimization
- [ ] Caching layer implementation
- [ ] Parallel processing enhancements
- [ ] Memory usage optimization

---

**LocalPulse** - Discover local business insights through data ✨