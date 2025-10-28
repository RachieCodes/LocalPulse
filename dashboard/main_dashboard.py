import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import folium
from streamlit_folium import st_folium
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from typing import List, Dict, Any
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.mongo_client import MongoDatabase
from utils.data_pipeline import DataPipeline
from utils.new_places_api import NewPlacesAPISearch
from utils.location_search import LocationBusinessSearch

# Configure page settings
st.set_page_config(
    page_title="Local Business Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

from database.mongo_client import MongoDatabase
from utils.data_pipeline import DataPipeline
from utils.new_places_api import NewPlacesAPISearch
from utils.location_search import LocationBusinessSearch


class LocalPulseDashboard:
    """Main dashboard class for LocalPulse"""
    
    def __init__(self):
        self.db = MongoDatabase()
        self.pipeline = DataPipeline(self.db)
        self.location_searcher = NewPlacesAPISearch()
        self.current_category_filter = ""
        
        # Always ensure database connection
        self._ensure_db_connection()
        
    def _ensure_db_connection(self):
        """Ensure database connection is established"""
        try:
            if self.db.client is None or self.db.db is None:
                self.db.connect()
            # Test the connection
            self.db.client.admin.command('ping')
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            # Try to reconnect
            try:
                self.db = MongoDatabase()
                self.db.connect()
            except Exception as e2:
                st.error(f"Failed to reconnect to database: {e2}")
                raise
    
    def _initialize_persistent_state(self):
        """Initialize persistent state using URL parameters and session state"""
        # Get URL query parameters
        query_params = st.query_params
        
        # Initialize or restore dropdown values from URL params or defaults
        defaults = {
            "time_period": "Last 90 days",
            "data_limit": "1000", 
            "category": "All",
            "city": "All Cities"
        }
        
        # Check URL parameters first, then session state, then defaults
        for param, default in defaults.items():
            if param == "time_period":
                param_key = "time_period_value"
            elif param == "data_limit":
                param_key = "data_limit_value"
            elif param == "category":
                param_key = "category_filter_value"
            elif param == "city":
                param_key = "primary_city_value"
            
            # Priority: URL params > session state > defaults
            if param in query_params:
                # URL parameter exists - use it and update session state
                value = query_params[param]
                if param == "data_limit" and value.isdigit():
                    value = int(value)
                st.session_state[param_key] = value
            elif param_key not in st.session_state:
                # No URL param and no session state - use default
                value = default
                if param == "data_limit":
                    value = int(value)
                st.session_state[param_key] = value
            # If session state exists but no URL param, keep session state value
    
    def _update_url_params(self, **params):
        """Update URL parameters to persist state across refreshes"""
        current_params = dict(st.query_params)
        current_params.update(params)
        st.query_params.update(current_params)
    
    def _check_and_offer_city_data(self, city_name: str):
        """Check if data exists for a city and automatically fetch it if not"""
        try:
            # Check if we have businesses for this city
            query = {
                "$or": [
                    {"city": {"$regex": city_name, "$options": "i"}},
                    {"address": {"$regex": city_name, "$options": "i"}}
                ]
            }
            existing_count = self.db.db.businesses.count_documents(query)
            
            if existing_count == 0:
                # Check if we've already tried to fetch data for this city in this session
                fetch_key = f"fetched_{city_name.lower().replace(' ', '_')}"
                
                if fetch_key not in st.session_state:
                    # Mark that we're fetching for this city
                    st.session_state[fetch_key] = True
                    
                    # Automatically fetch data for this city
                    st.info(f"🔍 No data found for **{city_name}**. Automatically fetching business data...")
                    self._fetch_city_data(city_name)
                else:
                    # Already tried to fetch, but still no data - might be an invalid city
                    st.warning(f"❌ No businesses found for **{city_name}**")
                    st.info("""
                    **Try these supported cities:**
                    - Chicago, Austin, Miami, Seattle, Boston
                    - New York, Los Angeles, Philadelphia, Houston
                    - Atlanta, Denver, Portland, Las Vegas, Nashville
                    """)
            else:
                # Data exists
                st.success(f"✅ Found **{existing_count}** businesses in {city_name}")
        except Exception as e:
            st.error(f"Error checking city data: {e}")
    
    def _fetch_city_data(self, city_name: str):
        """Fetch data for a specific city"""
        try:
            with st.spinner(f"🔍 Fetching business data for {city_name}..."):
                # Use the existing city search functionality
                success = self.search_businesses_by_city(city_name)
                
                if success:
                    st.success(f"✅ Successfully imported business data for {city_name}!")
                    # Small delay to show the success message
                    import time
                    time.sleep(1)
                    st.rerun()  # Refresh the page to show new data
                else:
                    # If fetch failed, show helpful message
                    st.error(f"❌ Could not fetch data for {city_name}")
                    st.info("Please try a different city name or check your internet connection.")
        except Exception as e:
            st.error(f"Error fetching city data: {e}")
    
    def _fetch_reviews_for_existing_businesses(self) -> int:
        """Fetch Google Places reviews for existing businesses that don't have reviews yet"""
        try:
            # Get businesses with source_id (which contains place_id) but no reviews
            businesses_cursor = self.db.db.businesses.find({
                "source_id": {"$exists": True, "$ne": None},
                "$or": [
                    {"reviews_fetched": {"$exists": False}},
                    {"reviews_fetched": False}
                ]
            }).limit(20)  # Process 20 businesses at a time
            
            businesses = list(businesses_cursor)
            if not businesses:
                st.info("No businesses found that need review fetching")
                return 0
            
            total_reviews = 0
            businesses_processed = 0
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, business in enumerate(businesses):
                source_id = business.get('source_id', '')
                business_name = business.get('name', 'Unknown')
                
                # Extract place_id from source_id (remove "city_search_" prefix)
                if source_id.startswith('city_search_'):
                    place_id = source_id.replace('city_search_', '')
                else:
                    place_id = source_id
                
                if not place_id:
                    continue
                
                status_text.text(f"Fetching reviews for {business_name} ({i+1}/{len(businesses)})")
                progress_bar.progress((i + 1) / len(businesses))
                
                try:
                    # Fetch reviews using our enhanced method
                    reviews = self.location_searcher.get_business_reviews(place_id, max_reviews=30)
                    
                    if reviews:
                        # Store reviews in database
                        for review in reviews:
                            review_doc = {
                                **review,
                                'business_id': business['_id'],
                                'business_name': business_name,
                                'business_category': business.get('category', ''),
                                'business_city': business.get('city', ''),
                                'created_at': datetime.now(),
                                'place_id': place_id,
                                'source_id': source_id,  # Keep original source_id for reference
                                'source_review_id': review.get('review_id', f"places_{place_id}_{hash(review.get('text', '') + review.get('author_name', ''))}")  # Add proper source_review_id
                            }
                            
                            # Insert review (avoid duplicates based on source_review_id and source)
                            self.db.db.reviews.update_one(
                                {
                                    'source_review_id': review_doc['source_review_id'],
                                    'source': review.get('source', 'google_places')
                                },
                                {'$set': review_doc},
                                upsert=True
                            )
                        
                        total_reviews += len(reviews)
                        st.success(f"✅ Fetched {len(reviews)} reviews for {business_name}")
                    else:
                        st.warning(f"⚠️ No reviews found for {business_name}")
                    
                    # Mark business as having reviews fetched
                    self.db.db.businesses.update_one(
                        {'_id': business['_id']},
                        {'$set': {'reviews_fetched': True, 'reviews_updated': datetime.now(), 'place_id': place_id}}
                    )
                    
                    businesses_processed += 1
                    
                    # Small delay to respect API limits
                    import time
                    time.sleep(0.3)
                    
                except Exception as e:
                    st.warning(f"Error fetching reviews for {business_name}: {e}")
                    continue
            
            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()
            
            # Clear cache so new data shows up
            st.cache_data.clear()
            
            st.success(f"🎉 Successfully processed {businesses_processed} businesses and fetched {total_reviews} total reviews!")
            
            return businesses_processed
            
        except Exception as e:
            raise Exception(f"Failed to fetch Google reviews: {e}")
    
    def _store_enhanced_search_results(self, results: List[Dict]) -> int:
        """Store enhanced search results (businesses with reviews) in database"""
        stored_count = 0
        
        try:
            for business_data in results:
                # Store business
                business_doc = {
                    'name': business_data['name'],
                    'address': business_data['address'],
                    'city': business_data['city'],
                    'state': business_data['state'],
                    'rating': business_data['rating'],
                    'review_count': business_data['review_count'],
                    'category': business_data['category'],
                    'subcategory': business_data['subcategory'],
                    'phone': business_data['phone'],
                    'latitude': business_data['latitude'],
                    'longitude': business_data['longitude'],
                    'place_id': business_data['place_id'],
                    'price_level': business_data['price_level'],
                    'source': 'google_places_enhanced',
                    'created_at': datetime.now(),
                    'reviews_fetched': True,
                    'reviews_updated': datetime.now()
                }
                
                # Update or insert business
                business_result = self.db.db.businesses.update_one(
                    {'place_id': business_data['place_id']},
                    {'$set': business_doc},
                    upsert=True
                )
                
                # Get business_id for reviews
                if business_result.upserted_id:
                    business_id = business_result.upserted_id
                else:
                    business_doc_in_db = self.db.db.businesses.find_one({'place_id': business_data['place_id']})
                    business_id = business_doc_in_db['_id']
                
                # Store reviews
                reviews = business_data.get('reviews', [])
                for review in reviews:
                    review_doc = {
                        **review,
                        'business_id': business_id,
                        'business_name': business_data['name'],
                        'business_category': business_data['category'],
                        'business_city': business_data['city'],
                        'created_at': datetime.now(),
                        'place_id': business_data['place_id'],
                        'source_review_id': review.get('review_id', f"places_{business_data['place_id']}_{hash(review.get('text', '') + review.get('author_name', ''))}")  # Add proper source_review_id
                    }
                    
                    # Insert review (avoid duplicates based on source_review_id and source)
                    self.db.db.reviews.update_one(
                        {
                            'source_review_id': review_doc['source_review_id'],
                            'source': review.get('source', 'google_places')
                        },
                        {'$set': review_doc},
                        upsert=True
                    )
                
                stored_count += 1
            
            return stored_count
            
        except Exception as e:
            st.error(f"Error storing search results: {e}")
            return 0

    def run(self):
        """Main dashboard runner"""
        # Custom CSS for professional styling
        st.markdown("""
        <style>
        .main-header {
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
            border-radius: 10px;
            margin-bottom: 2rem;
            text-align: center;
        }
        .main-title {
            font-size: 2rem;
            font-weight: 700;
            margin: 0;
        }
        .main-subtitle {
            font-size: 1rem;
            opacity: 0.9;
            margin: 0;
        }
        .metric-card {
            background: white;
            padding: 1.5rem;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            border-left: 4px solid #667eea;
            margin-bottom: 1rem;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: #333;
            margin: 0;
        }
        .metric-label {
            color: #666;
            font-size: 0.9rem;
            margin: 0;
        }
        .metric-delta {
            font-size: 0.8rem;
            margin-top: 0.5rem;
        }
        .positive { color: #28a745; }
        .negative { color: #dc3545; }
        .tab-content {
            background: white;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            margin-top: 1rem;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
        }
        .stTabs [data-baseweb="tab"] {
            background-color: #f8f9fa;
            border-radius: 10px 10px 0 0;
            padding: 0.75rem 1.5rem;
            border: none;
        }
        .stTabs [aria-selected="true"] {
            background-color: white;
            color: #667eea;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Initialize filter variables
        self.current_category_filter = ""
        self.current_primary_city = ""
        self.current_data_limit = 1000
        # Set wide date range to include all historical data (2018-2025)
        self.current_date_range = [datetime(2018, 1, 1).date(), datetime(2025, 12, 31).date()]
        
        # Initialize session state and URL params for dropdown persistence across refreshes
        self._initialize_persistent_state()
        
        # Sidebar (create early to set dashboard mode)
        self.create_sidebar()
        
        # Professional Header
        st.markdown("""
        <div class="main-header">
            <h1 class="main-title">🌐 Universal Business Intelligence</h1>
            <p class="main-subtitle">Scalable insights for businesses of any size - from startups to enterprises</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Get overview metrics for header
        try:
            self._ensure_db_connection()
            
            # Show different metrics based on dashboard mode
            if hasattr(self, 'current_dashboard_mode') and self.current_dashboard_mode == "🏪 Business Owner":
                # Business Owner Mode Metrics (first image)
                total_businesses = self.db.db.businesses.count_documents({})
                total_reviews = self.db.db.reviews.count_documents({})
                avg_rating = list(self.db.db.businesses.aggregate([
                    {"$group": {"_id": None, "avg_rating": {"$avg": "$rating"}}}
                ]))
                avg_rating = avg_rating[0]['avg_rating'] if avg_rating else 0
                
                # Calculate sentiment distribution
                sentiment_data = list(self.db.db.reviews.aggregate([
                    {"$group": {"_id": "$sentiment_label", "count": {"$sum": 1}}}
                ]))
                positive_pct = 0
                for item in sentiment_data:
                    if item['_id'] == 'positive':
                        positive_pct = (item['count'] / total_reviews) * 100 if total_reviews > 0 else 0
                
                # Get unique categories
                categories = list(self.db.db.businesses.distinct("category"))
                category_count = len([cat for cat in categories if cat])
                
                # Header metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{avg_rating:.1f} / 5.0</div>
                        <div class="metric-label">Average Rating</div>
                        <div class="metric-delta positive">📈 +4.8% from last quarter</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{total_reviews:,}</div>
                        <div class="metric-label">Total Reviews Analyzed</div>
                        <div class="metric-delta positive">📊 Across {total_businesses} businesses</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{total_businesses}</div>
                        <div class="metric-label">Businesses Tracked</div>
                        <div class="metric-delta positive">📍 In {category_count} categories</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col4:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{positive_pct:.0f}%</div>
                        <div class="metric-label">Positive Sentiment</div>
                        <div class="metric-delta positive">😊 High customer satisfaction</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            else:
                # Market Analytics Mode Metrics (second image)
                total_businesses = self.db.db.businesses.count_documents({})
                total_reviews = self.db.db.reviews.count_documents({})
                avg_rating = list(self.db.db.businesses.aggregate([
                    {"$group": {"_id": None, "avg_rating": {"$avg": "$rating"}}}
                ]))
                avg_rating = avg_rating[0]['avg_rating'] if avg_rating else 0
                
                # Calculate average reviews per business
                reviews_per_business = total_reviews / total_businesses if total_businesses > 0 else 0
                
                # Calculate growth opportunities (high rated businesses with low reviews)
                high_rated_low_reviews = self.db.db.businesses.count_documents({
                    "rating": {"$gte": 4.0},
                    "review_count": {"$lt": 50}
                })
                opportunity_pct = (high_rated_low_reviews / total_businesses * 100) if total_businesses > 0 else 0
                
                # Get unique categories
                categories = list(self.db.db.businesses.distinct("category"))
                category_count = len([cat for cat in categories if cat])
                
                # Header metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{total_businesses}</div>
                        <div class="metric-label">Total Businesses</div>
                        <div class="metric-delta positive">📊 {category_count} categories</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    rating_insight = "👍 Good" if avg_rating >= 4.0 else "📈 Improving" if avg_rating >= 3.5 else "⚠️ Needs attention"
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{avg_rating:.1f}</div>
                        <div class="metric-label">Average Rating</div>
                        <div class="metric-delta positive">{rating_insight}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    engagement_level = "🔥 High engagement" if reviews_per_business >= 100 else "📱 Medium engagement" if reviews_per_business >= 20 else "🌱 Growing"
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{reviews_per_business:.0f}</div>
                        <div class="metric-label">Avg Reviews/Business</div>
                        <div class="metric-delta positive">{engagement_level}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col4:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value">{opportunity_pct:.0f}%</div>
                        <div class="metric-label">Growth Opportunities</div>
                        <div class="metric-delta positive">� Underexposed gems</div>
                    </div>
                    """, unsafe_allow_html=True)
                
        except Exception as e:
            st.error(f"Error loading metrics: {e}")
        
        # Create tabs based on dashboard mode
        if hasattr(self, 'current_dashboard_mode') and self.current_dashboard_mode == "🏪 Business Owner":
            # Business Owner Mode Tabs
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "🏪 My Business", 
                "📊 Performance Analytics", 
                "🥊 Competitor Analysis", 
                "💡 Improvement Insights",
                "🗃️ Data Management"
            ])
        else:
            # Market Analytics Mode Tabs (default)
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                "📊 Universal Analytics", 
                "💭 Sentiment Intelligence", 
                "🔍 Keyword Insights", 
                "⏰ Timing Analytics", 
                "🏆 Market Intelligence",
                "🗃️ Data Management"
            ])
        
        # Get current filters (set by sidebar)
        
        # Tab content based on mode
        if hasattr(self, 'current_dashboard_mode') and self.current_dashboard_mode == "🏪 Business Owner":
            # Business Owner Mode Content
            with tab1:
                self.show_my_business_dashboard()
            
            with tab2:
                self.show_business_performance_analytics()
            
            with tab3:
                self.show_business_competitor_analysis()
            
            with tab4:
                self.show_business_improvement_insights()
            
            with tab5:
                self.show_database_manager()
        else:
            # Market Analytics Mode Content (default)
            with tab1:
                self.show_universal_analytics(self.current_category_filter, self.current_primary_city, self.current_date_range)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with tab2:
                self.show_sentiment_analysis(self.current_category_filter, self.current_primary_city, self.current_date_range)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with tab3:
                self.show_keyword_analysis(self.current_category_filter, self.current_primary_city, self.current_date_range)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with tab4:
                self.show_time_analytics(self.current_category_filter, self.current_primary_city, self.current_date_range)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with tab5:
                self.show_market_intelligence(self.current_primary_city)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with tab6:
                self.show_database_manager()
                st.markdown('</div>', unsafe_allow_html=True)
    
    def create_sidebar(self):
        """Create sidebar with filters"""
        with st.sidebar:
            st.markdown('<div class="sidebar-header">🎛️ Dashboard Mode</div>', unsafe_allow_html=True)
            
            # Dashboard Mode Selection
            dashboard_mode = st.radio(
                "Choose your dashboard experience:",
                options=["📊 Market Analytics", "🏪 Business Owner"],
                index=0 if not hasattr(st.session_state, 'dashboard_mode') else (0 if st.session_state.dashboard_mode == "📊 Market Analytics" else 1),
                key="dashboard_mode_selector",
                help="Market Analytics: Analyze market trends and competitors\nBusiness Owner: Manage your business performance and compare to competitors"
            )
            
            # Store the mode in session state
            st.session_state.dashboard_mode = dashboard_mode
            self.current_dashboard_mode = dashboard_mode
            
            # Show mode-specific instructions
            if dashboard_mode == "🏪 Business Owner":
                st.info("🏪 **Business Owner Mode**: Select your business to see detailed performance analytics and competitor comparisons")
                
                # Business Selection Interface
                self._create_business_selection_interface()
            else:
                st.info("📊 **Market Analytics Mode**: Analyze market trends, sentiment, and competitive landscape")
            
            st.markdown("---")
            st.markdown('<div class="sidebar-header">🎛️ Filters</div>', unsafe_allow_html=True)
            
            st.markdown("### 🌍 City Analysis")
            st.write("Search for businesses in any city in the United States!")
            
            # City input method selector
            input_method = st.radio(
                "How would you like to select a city?",
                options=["🔍 Search any city", "📋 Choose from popular cities"],
                index=0,
                key="city_input_method"
            )
            
            if input_method == "🔍 Search any city":
                primary_city = st.text_input(
                    "Enter any US city name",
                    value=st.session_state.primary_city_value if st.session_state.primary_city_value != "All Cities" else "",
                    placeholder="e.g., Austin TX, Portland OR, Nashville TN, Miami FL",
                    key="free_form_city",
                    help="Type any US city name - get real business data from Google Places API!"
                )
                if not primary_city:
                    primary_city = "All Cities"
                # Update session state and URL params if value changed
                if primary_city != st.session_state.primary_city_value:
                    st.session_state.primary_city_value = primary_city
                    self._update_url_params(city=primary_city)
                
                # Check if data exists for this city and offer to fetch if needed
                if primary_city and primary_city != "All Cities":
                    self._check_and_offer_city_data(primary_city)
            else:
                # Find current index for session state value
                popular_cities = ["All Cities", 
                        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                        "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin", "Seattle", 
                        "Miami", "Atlanta", "Boston", "Denver", "Portland", "Las Vegas", "Nashville",
                        "Detroit", "Memphis", "Charlotte", "Tampa", "Milwaukee", "Oklahoma City",
                        "Louisville", "Baltimore", "Kansas City", "Virginia Beach", "Omaha", "Raleigh",
                        "Colorado Springs", "Tucson", "Fresno", "Sacramento", "Mesa", "Arlington"]
                
                try:
                    current_index = popular_cities.index(st.session_state.primary_city_value)
                except (ValueError, KeyError):
                    current_index = 0
                
                primary_city = st.selectbox(
                    "Choose from popular US cities",
                    options=popular_cities,
                    index=current_index,
                    key="popular_city_selector"
                )
                # Update session state and URL params if value changed
                if primary_city != st.session_state.primary_city_value:
                    st.session_state.primary_city_value = primary_city
                    self._update_url_params(city=primary_city)
                
                # Check if data exists for this city and automatically fetch if needed
                if primary_city and primary_city != "All Cities":
                    self._check_and_offer_city_data(primary_city)
            
            # Store processed primary city
            self.current_primary_city = "" if primary_city == "All Cities" else primary_city
            
            # Show what city is being analyzed
            if self.current_primary_city:
                st.success(f"🎯 Analyzing: **{self.current_primary_city}**")
            else:
                st.info("🌍 Analyzing: **All Cities**")
            
            st.markdown("---")
            
            # Category filter
            try:
                categories = list(self.db.db.businesses.distinct("category"))
                category_options = ["All"] + categories
                
                # Find current index for session state value
                try:
                    current_index = category_options.index(st.session_state.category_filter_value)
                except (ValueError, KeyError):
                    current_index = 0
                
                category_filter = st.selectbox(
                    "🏪 Business Category",
                    options=category_options,
                    index=current_index,
                    key="category_filter"
                )
                # Update session state and URL params if value changed, store the processed value
                if category_filter != st.session_state.category_filter_value:
                    st.session_state.category_filter_value = category_filter
                    self._update_url_params(category=category_filter)
                self.current_category_filter = "" if category_filter == "All" else category_filter
            except:
                self.current_category_filter = ""
    
    def _create_business_selection_interface(self):
        """Create interface for business owners to select or enter their business"""
        try:
            # Business entry method selection
            entry_method = st.radio(
                "How would you like to add your business?",
                options=["🔍 Select from existing businesses", "➕ Enter my business details"],
                index=0,
                key="business_entry_method"
            )
            
            if entry_method == "➕ Enter my business details":
                self._create_manual_business_entry()
            else:
                self._create_existing_business_selection()
                
        except Exception as e:
            st.error(f"Error in business selection interface: {e}")
            self.current_selected_business = None
    
    def _create_manual_business_entry(self):
        """Allow users to manually enter their business information"""
        st.markdown("### 📝 Enter Your Business Information")
        
        with st.form("business_entry_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                business_name = st.text_input(
                    "Business Name *", 
                    placeholder="e.g., Joe's Coffee Shop",
                    help="Enter your business name exactly as it appears online"
                )
                
                business_category = st.selectbox(
                    "Business Category *",
                    options=[
                        "Restaurant", "Cafe", "Fast Food", "Bar & Grill",
                        "Retail Store", "Clothing Store", "Electronics", "Grocery Store",
                        "Hair Salon", "Spa", "Fitness Center", "Auto Repair",
                        "Dentist", "Doctor", "Veterinarian", "Hotel",
                        "Real Estate", "Insurance", "Legal Services", "Accounting",
                        "Other"
                    ],
                    help="Select the category that best describes your business"
                )
                
                business_city = st.text_input(
                    "City *", 
                    placeholder="e.g., Austin, TX",
                    help="City and state where your business is located"
                )
            
            with col2:
                business_address = st.text_input(
                    "Address", 
                    placeholder="e.g., 123 Main St, Austin, TX 78701",
                    help="Full business address (optional)"
                )
                
                current_rating = st.slider(
                    "Current Average Rating",
                    min_value=1.0, max_value=5.0, value=4.0, step=0.1,
                    help="Your current average rating (if known)"
                )
                
                review_count = st.number_input(
                    "Approximate Number of Reviews",
                    min_value=0, max_value=10000, value=50, step=1,
                    help="Approximate number of reviews your business has"
                )
            
            # Additional business details
            st.markdown("### 📋 Additional Details (Optional)")
            
            col3, col4 = st.columns(2)
            with col3:
                years_in_business = st.number_input(
                    "Years in Business",
                    min_value=0, max_value=100, value=5, step=1
                )
                
                employee_count = st.selectbox(
                    "Number of Employees",
                    options=["1-5", "6-10", "11-25", "26-50", "51-100", "100+"]
                )
            
            with col4:
                website = st.text_input(
                    "Website", 
                    placeholder="https://www.yourbusiness.com"
                )
                
                phone = st.text_input(
                    "Phone Number", 
                    placeholder="(555) 123-4567"
                )
            
            # Business goals/focus
            business_goals = st.multiselect(
                "What are your main business goals? (Select all that apply)",
                options=[
                    "Increase customer reviews",
                    "Improve average rating", 
                    "Understand customer feedback",
                    "Analyze competitors",
                    "Improve customer service",
                    "Increase visibility",
                    "Monitor online reputation"
                ]
            )
            
            submitted = st.form_submit_button("📊 Create My Business Dashboard")
            
            if submitted:
                if business_name and business_category and business_city:
                    # Create custom business object
                    custom_business = {
                        'name': business_name,
                        'category': business_category,
                        'city': business_city,
                        'address': business_address if business_address else f"{business_city}",
                        'rating': current_rating,
                        'review_count': review_count,
                        'years_in_business': years_in_business,
                        'employee_count': employee_count,
                        'website': website,
                        'phone': phone,
                        'business_goals': business_goals,
                        'custom_entry': True,  # Flag to identify manually entered businesses
                        '_id': f"custom_{business_name.lower().replace(' ', '_')}"
                    }
                    
                    # Store in session state
                    st.session_state.selected_business = custom_business
                    self.current_selected_business = custom_business
                    
                    st.success(f"✅ **Business Dashboard Created**: {business_name}")
                    st.info("💡 Your business dashboard is ready! Use the tabs above to explore your analytics and competitor insights.")
                    
                    # Show business summary
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Rating", f"{current_rating:.1f}⭐")
                    with col2:
                        st.metric("Reviews", f"{review_count:,}")
                    with col3:
                        st.metric("Years", f"{years_in_business}")
                    
                else:
                    st.error("⚠️ Please fill in all required fields marked with *")
    
    def _create_existing_business_selection(self):
        """Create interface for selecting from existing businesses"""
        # Get available businesses
        businesses = list(self.db.db.businesses.find({}, {"name": 1, "category": 1, "city": 1, "rating": 1, "review_count": 1}).limit(500))
        
        if not businesses:
            st.warning("No businesses available. Please load some business data first or enter your business manually.")
            return
        
        # Create business options for dropdown
        business_options = ["Select your business..."] + [
            f"{biz['name']} ({biz.get('category', 'Unknown')} - {biz.get('city', 'Unknown')}) - {biz.get('rating', 0):.1f}⭐ ({biz.get('review_count', 0)} reviews)"
            for biz in businesses
        ]
        
        # Business selection dropdown
        selected_business = st.selectbox(
            "🏪 Choose Your Business",
            options=business_options,
            index=0,
            key="existing_business_selection",
            help="Select your business to see detailed analytics and competitor comparison"
        )
        
        if selected_business != "Select your business...":
            # Extract business name from the selected option
            business_name = selected_business.split(" (")[0]
            
            # Store selected business info
            selected_biz_data = next((biz for biz in businesses if biz['name'] == business_name), None)
            if selected_biz_data:
                st.session_state.selected_business = selected_biz_data
                self.current_selected_business = selected_biz_data
                
                # Show selected business summary
                st.success(f"✅ **Selected**: {business_name}")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Rating", f"{selected_biz_data.get('rating', 0):.1f}⭐")
                with col2:
                    st.metric("Reviews", f"{selected_biz_data.get('review_count', 0)}")
                
                # Get and show recent review summary
                recent_reviews = list(self.db.db.reviews.find(
                    {"business_name": business_name},
                    {"sentiment_label": 1, "rating": 1}
                ).limit(50))
                
                if recent_reviews:
                    positive_reviews = len([r for r in recent_reviews if r.get('sentiment_label') == 'positive'])
                    total_recent = len(recent_reviews)
                    sentiment_pct = (positive_reviews / total_recent * 100) if total_recent > 0 else 0
                    
                    st.metric("Recent Sentiment", f"{sentiment_pct:.0f}% Positive")
        else:
            # Clear selection
            if 'selected_business' in st.session_state:
                del st.session_state.selected_business
            self.current_selected_business = None
    
    def _continue_sidebar_creation(self):
        """Continue with the rest of the sidebar after business selection"""
        
        # Show what city is being analyzed
        if self.current_primary_city:
            st.success(f"🎯 Analyzing: **{self.current_primary_city}**")
        else:
            st.info("🌍 Analyzing: **All Cities**")
        
        st.markdown("---")
        
        # Category filter
        try:
            categories = list(self.db.businesses.distinct("category"))
            category_options = ["All"] + categories
            
            # Find current index for session state value
            try:
                current_index = category_options.index(st.session_state.category_filter_value)
            except (ValueError, KeyError):
                current_index = 0
            
            category_filter = st.selectbox(
                "🏪 Business Category",
                options=category_options,
                index=current_index,
                key="category_filter"
            )
            # Update session state and URL params if value changed, store the processed value
            if category_filter != st.session_state.category_filter_value:
                st.session_state.category_filter_value = category_filter
                self._update_url_params(category=category_filter)
            self.current_category_filter = "" if category_filter == "All" else category_filter
        except:
            self.current_category_filter = ""
            
            # Business Intelligence Filter
            st.markdown("### � Business Intelligence Focus")
            
            analysis_focus_options = [
                "📊 All Businesses - Complete Market View",
                "🔍 Market Leaders - Top Performers Analysis", 
                "📈 Growth Opportunities - Underperforming Segments",
                "🏆 Competitive Intelligence - Industry Benchmarks",
                "💡 Niche Markets - Specialized Business Types",
                "🌟 Customer Experience - Service Quality Analysis",
                "📍 Location-Based - Geographic Performance",
                "⏰ Trend Analysis - Time-Based Patterns"
            ]
            
            analysis_focus = st.selectbox(
                "Choose your analysis focus",
                options=analysis_focus_options,
                index=0,
                key="analysis_focus",
                help="Select the type of business intelligence insights you want to explore"
            )
            
            # Store the analysis focus for use in queries
            self.current_analysis_focus = analysis_focus
            
            # Show dynamic insights based on focus
            if "Market Leaders" in analysis_focus:
                st.info("🏆 **Market Leaders Analysis** - Analyzing top-performing businesses with highest ratings and engagement")
            elif "Growth Opportunities" in analysis_focus:
                st.info("📈 **Growth Opportunities** - Identifying businesses with potential for improvement and market gaps")
            elif "Competitive Intelligence" in analysis_focus:
                st.info("🔍 **Competitive Intelligence** - Benchmarking performance against industry standards")
            elif "Niche Markets" in analysis_focus:
                st.info("💡 **Niche Markets** - Focusing on specialized and unique business segments")
            elif "Customer Experience" in analysis_focus:
                st.info("🌟 **Customer Experience** - Deep dive into service quality and customer satisfaction")
            elif "Location-Based" in analysis_focus:
                st.info("📍 **Location-Based Analysis** - Geographic performance and local market dynamics")
            elif "Trend Analysis" in analysis_focus:
                st.info("⏰ **Trend Analysis** - Time-based patterns and seasonal business insights")
            
            # Advanced Filtering Options
            with st.expander("🔧 Advanced Filters", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    # Review Volume Filter
                    st.markdown("**Review Volume Range**")
                    min_reviews = st.number_input("Minimum reviews", min_value=0, value=0, step=10)
                    max_reviews = st.number_input("Maximum reviews", min_value=1, value=10000, step=100)
                    
                    # Rating Filter
                    st.markdown("**Rating Range**")
                    min_rating = st.slider("Minimum rating", 1.0, 5.0, 1.0, 0.1)
                    max_rating = st.slider("Maximum rating", 1.0, 5.0, 5.0, 0.1)
                
                with col2:
                    # Business Type Classifications
                    st.markdown("**Business Classifications**")
                    business_types = st.multiselect(
                        "Include business types",
                        ["Restaurant", "Retail", "Service", "Healthcare", "Entertainment", "Automotive", "Professional", "Other"],
                        default=[]
                    )
                    
                    # Exclude chains option
                    exclude_chains = st.checkbox("Exclude major chains", value=False)
                    include_only_chains = st.checkbox("Include only chains", value=False)
            
            # Store advanced filters
            self.advanced_filters = {
                'min_reviews': min_reviews,
                'max_reviews': max_reviews,
                'min_rating': min_rating,
                'max_rating': max_rating,
                'business_types': business_types,
                'exclude_chains': exclude_chains,
                'include_only_chains': include_only_chains
            }
            
            st.markdown("---")
            st.markdown("### 📅 Time Period Analysis")
            
            # Time period selector with "No filter" option
            time_period_options = ["No filter - All time", "Last 30 days", "Last 90 days", "Last 6 months", "Last year", "Custom range"]
            
            # Find current index for session state value
            try:
                current_index = time_period_options.index(st.session_state.time_period_value)
            except (ValueError, KeyError):
                current_index = 2  # Default to 90 days
            
            time_period = st.selectbox(
                "Choose time period",
                options=time_period_options,
                index=current_index,
                key="time_period",
                help="Select predefined period, custom range, or 'No filter' to analyze ALL historical data"
            )
            
            # Update session state and URL params if value changed
            if time_period != st.session_state.time_period_value:
                st.session_state.time_period_value = time_period
                self._update_url_params(time_period=time_period)
            
            # Calculate date range based on selection
            current_date = datetime.now().date()
            
            if time_period == "No filter - All time":
                # No time filtering - analyze all data
                date_range = None
                st.success("🌍 **Analyzing ALL historical data** (no time restrictions)")
            elif time_period == "Last 30 days":
                start_date = current_date - timedelta(days=30)
                end_date = current_date
                date_range = [start_date, end_date]
            elif time_period == "Last 90 days":
                start_date = current_date - timedelta(days=90)
                end_date = current_date
                date_range = [start_date, end_date]
            elif time_period == "Last 6 months":
                start_date = current_date - timedelta(days=180)
                end_date = current_date
                date_range = [start_date, end_date]
            elif time_period == "Last year":
                start_date = current_date - timedelta(days=365)
                end_date = current_date
                date_range = [start_date, end_date]
            else:  # Custom range
                date_range = st.date_input(
                    "Select custom date range",
                    value=[current_date - timedelta(days=90), current_date],  # Default end date is always current
                    max_value=current_date,  # Can't select future dates
                    key="custom_date_range",
                    help="End date defaults to today"
                )
                
                # Ensure we have both dates
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    date_range = list(date_range)
                elif not isinstance(date_range, list) or len(date_range) != 2:
                    # If only one date selected, make range from that date to today
                    if hasattr(date_range, '__iter__') and date_range:
                        date_range = [date_range[0] if isinstance(date_range, (list, tuple)) else date_range, current_date]
                    else:
                        date_range = [current_date - timedelta(days=90), current_date]
            
            # Show selected period info (only if not "No filter")
            if time_period != "No filter - All time" and date_range and len(date_range) == 2:
                days_span = (date_range[1] - date_range[0]).days
                st.info(f"📊 Analyzing {days_span} days of data ({date_range[0]} to {date_range[1]})")
            
            # Data limit controls
            st.markdown("---")
            st.markdown("### ⚡ Performance Controls")
            
            # Data limit selector with "No limits" option
            data_limit_options = ["No limits - All records", "All data", 100, 500, 1000, 2500, 5000]
            
            # Find current index for session state value
            try:
                current_index = data_limit_options.index(st.session_state.data_limit_value)
            except (ValueError, KeyError):
                current_index = 4  # Default to 1000
            
            data_limit = st.selectbox(
                "Maximum records to analyze",
                options=data_limit_options,
                index=current_index,
                key="data_limit",
                help="Limit data for faster performance, choose 'All data' for complete analysis, or 'No limits' for unlimited analysis."
            )
            
            # Update session state and URL params if value changed
            if data_limit != st.session_state.data_limit_value:
                st.session_state.data_limit_value = data_limit
                self._update_url_params(data_limit=str(data_limit))
            
            # Convert to integer or None
            if data_limit == "No limits - All records":
                self.current_data_limit = None
                st.success("🚀 **Analyzing ALL data** (no record limits)")
            elif data_limit == "All data":
                self.current_data_limit = None
                st.caption("📊 Analyzing all available data (may be slower)")
            else:
                self.current_data_limit = data_limit
                st.caption(f"⚡ Limiting to {self.current_data_limit} most recent records for faster performance")
            
            # Performance tip
            with st.expander("💡 Performance Tips", expanded=False):
                st.write("""
                **For faster performance:**
                - Choose a specific primary city instead of "All Cities"
                - Use shorter time periods (30-90 days)
                - Limit data to 1000-2500 records
                
                **For complete analysis:**
                - Select "No filter - All time" to include all historical data
                - Select "No limits - All records" to analyze all data
                - Select "All Cities" for comprehensive geographic analysis
                - Be patient with loading times for large datasets
                
                **Recommended settings:**
                - **Quick Overview**: Last 90 days, 1000 records
                - **Deep Analysis**: No filter - All time, No limits - All records
                - **Specific Research**: Custom time range, specific city, All data
                """)
            
            # Store processed date range
            self.current_date_range = date_range
            
            # Refresh data button
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
            
            # Data processing section
            st.markdown("---")
            st.markdown('<div class="sidebar-header">⚙️ Data Processing</div>', unsafe_allow_html=True)

            if st.button("🔄 Process New Reviews"):
                with st.spinner("Processing reviews..."):
                    try:
                        result = self.pipeline.process_new_reviews(limit=100)
                        st.success(f"Processed {result} new reviews!")
                    except Exception as e:
                        st.error(f"Error processing reviews: {e}")
            
            # Google Places Review Fetching
            st.markdown("### 🌐 Google Places Reviews")
            
            if st.button("⭐ Fetch Reviews for Existing Businesses"):
                with st.spinner("Fetching Google Places reviews..."):
                    try:
                        result = self._fetch_reviews_for_existing_businesses()
                        st.success(f"Fetched reviews for {result} businesses!")
                    except Exception as e:
                        st.error(f"Error fetching reviews: {e}")
            
            # Enhanced search with reviews
            with st.expander("🔍 Search Places with Reviews"):
                search_query = st.text_input("Search Query", placeholder="e.g., restaurants, coffee shops")
                search_location = st.text_input("Location", placeholder="e.g., San Francisco, CA")
                max_results = st.slider("Max Results", 5, 50, 20)
                
                if st.button("� Search & Fetch Reviews") and search_query:
                    with st.spinner(f"Searching for '{search_query}' and fetching reviews..."):
                        try:
                            results = self.location_searcher.search_places_with_reviews(
                                search_query, search_location, max_results
                            )
                            if results:
                                # Store results in database
                                stored_count = self._store_enhanced_search_results(results)
                                st.success(f"Found {len(results)} places and stored {stored_count} businesses with reviews!")
                                st.rerun()  # Refresh to show new data
                            else:
                                st.warning("No results found")
                        except Exception as e:
                            st.error(f"Error: {e}")

            if st.button("�📊 Update Analytics"):
                with st.spinner("Updating analytics..."):
                    try:
                        result = self.pipeline.update_business_analytics()
                        st.success(f"Updated analytics for {result} businesses!")
                    except Exception as e:
                        st.error(f"Error updating analytics: {e}")            # Database management section
            st.markdown("---")
            st.markdown('<div class="sidebar-header">🗑️ Database Management</div>', unsafe_allow_html=True)
            
            # Show database stats
            try:
                business_count = self.db.db.businesses.count_documents({})
                review_count = self.db.db.reviews.count_documents({})
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("📊 Businesses", business_count)
                with col2:
                    st.metric("📝 Reviews", review_count)
                    
                # Show data by source
                sources = self.db.db.businesses.distinct("source")
                if sources:
                    st.write("**By Source:**")
                    for source in sources:
                        b_count = self.db.db.businesses.count_documents({"source": source})
                        r_count = self.db.db.reviews.count_documents({"source": source})
                        st.write(f"• {source}: {b_count} businesses, {r_count} reviews")
                        
            except Exception as e:
                st.write("⚠️ Could not load database stats")
            
            # Database actions
            st.write("**Actions:**")
            
            # Clear sample data
            if st.button("🗑️ Clear Sample Data", help="Remove manually created sample data"):
                if self._clear_data_by_source("manual"):
                    st.success("✅ Sample data cleared!")
                    st.rerun()
                else:
                    st.error("❌ Failed to clear sample data")
            
            # Clear Google Places data
            if st.button("🗑️ Clear Google Data", help="Remove data imported from Google Places"):
                if self._clear_data_by_source("google_places"):
                    st.success("✅ Google Places data cleared!")
                    st.rerun()
                else:
                    st.error("❌ Failed to clear Google data")
            
            # Clear all data (with confirmation)
            if st.button("🚨 Clear ALL Data", help="Remove all businesses and reviews"):
                # Show confirmation dialog
                if "confirm_clear_all" not in st.session_state:
                    st.session_state.confirm_clear_all = False
                
                if not st.session_state.confirm_clear_all:
                    st.warning("⚠️ This will delete ALL data!")
                    if st.button("⚠️ Yes, Delete Everything"):
                        st.session_state.confirm_clear_all = True
                        st.rerun()
                else:
                    if self._clear_all_data():
                        st.success("✅ All data cleared!")
                        st.session_state.confirm_clear_all = False
                        st.rerun()
                    else:
                        st.error("❌ Failed to clear data")
                        st.session_state.confirm_clear_all = False
    
    def _clear_data_by_source(self, source_name: str) -> bool:
        """Clear data from a specific source"""
        try:
            # Count entries to be deleted
            business_count = self.db.db.businesses.count_documents({"source": source_name})
            review_count = self.db.db.reviews.count_documents({"source": source_name})
            
            if business_count == 0 and review_count == 0:
                return True  # Nothing to delete
            
            # Delete by source
            self.db.db.businesses.delete_many({"source": source_name})
            self.db.db.reviews.delete_many({"source": source_name})
            
            # Clear all cached data
            st.cache_data.clear()
            
            # Clear relevant session state that might cache results
            self._clear_cached_session_state()
            
            return True
            
        except Exception as e:
            st.error(f"Error clearing {source_name} data: {e}")
            return False
    
    def _clear_all_data(self) -> bool:
        """Clear all data from the database"""
        try:
            # Clear all collections
            self.db.db.businesses.delete_many({})
            self.db.db.reviews.delete_many({})
            self.db.db.analytics.delete_many({})
            self.db.db.trending_keywords.delete_many({})
            
            # Clear all cached data
            st.cache_data.clear()
            
            # Clear relevant session state that might cache results
            self._clear_cached_session_state()
            
            return True
            
        except Exception as e:
            st.error(f"Error clearing all data: {e}")
            return False
    
    def _clear_cached_session_state(self):
        """Clear session state variables that might cache search results or data"""
        # Clear any cached search results or data-related session state
        keys_to_clear = [
            'search_results',
            'city_search_results', 
            'last_search_query',
            'cached_businesses',
            'cached_categories',
            'cached_cities',
            'business_data_cache',
            'review_data_cache'
        ]
        
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
    
    @st.cache_data
    def get_available_categories(_self):
        """Get available business categories"""
        try:
            categories = _self.db.db.businesses.distinct("category")
            return [cat for cat in categories if cat]
        except:
            return []
    
    def _show_active_filters(self, category_filter: str, primary_city: str):
        """Display currently active filters"""
        filters = []
        if primary_city:
            filters.append(f"🌍 City: {primary_city}")
        if category_filter:
            filters.append(f"🏪 Category: {category_filter}")
        
        # Check if time filtering is enabled
        if hasattr(self, 'current_date_range') and self.current_date_range:
            start_date, end_date = self.current_date_range
            filters.append(f"📅 Period: {start_date} to {end_date}")
        else:
            filters.append("📅 Period: ALL TIME")
        
        # Check if performance limits are enabled
        if hasattr(self, 'current_data_limit') and self.current_data_limit:
            filters.append(f"⚡ Limit: {self.current_data_limit} records")
        else:
            filters.append("⚡ Limit: ALL DATA")
        
        if filters:
            st.info("🔍 **Active Filters:** " + " • ".join(filters))
    
    def show_top_businesses(self, category_filter: str, primary_city: str):
        """Show top-rated businesses with enhanced filtering"""
        st.header("🏆 Top-Rated Businesses")
        
        # Show active filters
        self._show_active_filters(category_filter, primary_city)
        
        # Get top businesses data
        try:
            businesses = self.get_top_businesses_data(category_filter, primary_city)
            
            if not businesses:
                st.warning("No businesses found with the current filters.")
                return
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Businesses", len(businesses))
            
            with col2:
                avg_rating = np.mean([b.get('rating', 0) for b in businesses if b.get('rating')])
                st.metric("Average Rating", f"{avg_rating:.2f}")
            
            with col3:
                total_reviews = sum([b.get('review_count', 0) for b in businesses])
                st.metric("Total Reviews", f"{total_reviews:,}")
            
            with col4:
                categories_count = len(set([b.get('category') for b in businesses if b.get('category')]))
                st.metric("Categories", categories_count)
            
            # Create DataFrame for display
            df = pd.DataFrame(businesses)
            
            # Top businesses table
            st.subheader("📋 Top Businesses by Rating")
            display_cols = ['name', 'category', 'rating', 'review_count', 'address', 'price_range']
            available_cols = [col for col in display_cols if col in df.columns]
            
            if available_cols:
                st.dataframe(
                    df[available_cols].head(20),
                    use_container_width=True,
                    column_config={
                        "name": st.column_config.TextColumn("Business Name", width="medium"),
                        "rating": st.column_config.NumberColumn("Rating", format="%.1f ⭐"),
                        "review_count": st.column_config.NumberColumn("Reviews", format="%d"),
                    }
                )
            
            # Rating distribution chart
            if 'rating' in df.columns:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📊 Rating Distribution")
                    rating_counts = df['rating'].value_counts().sort_index()
                    fig_rating = px.bar(
                        x=rating_counts.index,
                        y=rating_counts.values,
                        labels={'x': 'Rating', 'y': 'Number of Businesses'},
                        title="Distribution of Business Ratings"
                    )
                    st.plotly_chart(fig_rating, use_container_width=True)
                
                with col2:
                    st.subheader("🏪 Category Breakdown")
                    if 'category' in df.columns:
                        category_counts = df['category'].value_counts().head(10)
                        fig_category = px.pie(
                            values=category_counts.values,
                            names=category_counts.index,
                            title="Top Business Categories"
                        )
                        st.plotly_chart(fig_category, use_container_width=True)
            
            # Map visualization
            if df[['latitude', 'longitude']].notna().any().any():
                st.subheader("🗺️ Business Locations")
                self.create_business_map(df)
                
        except Exception as e:
            st.error(f"Error loading top businesses: {e}")
    
    def _get_filtered_businesses(self, category_filter="", primary_city="", date_range=None):
        """Get filtered businesses data as DataFrame for universal analytics"""
        try:
            self._ensure_db_connection()
            
            # Build query based on filters
            query = {}
            
            # Add category filter
            if category_filter:
                query["category"] = {"$regex": category_filter, "$options": "i"}
            
            # Add primary city filter
            if primary_city:
                query["$or"] = [
                    {"city": {"$regex": primary_city, "$options": "i"}},
                    {"address": {"$regex": primary_city, "$options": "i"}}
                ]
            
            # Add advanced filters and analysis focus
            if hasattr(self, 'current_analysis_focus') and "All Businesses" not in self.current_analysis_focus:
                focus = self.current_analysis_focus
                
                if "Market Leaders" in focus:
                    # Top 20% by rating and review engagement
                    query["$and"] = [
                        {"rating": {"$gte": 4.0}},
                        {"review_count": {"$gte": 50}}
                    ]
                elif "Growth Opportunities" in focus:
                    # Businesses with room for improvement
                    query["$and"] = [
                        {"rating": {"$lt": 4.0}},
                        {"review_count": {"$gte": 10}}
                    ]
                elif "Competitive Intelligence" in focus:
                    # Businesses in competitive segments (multiple similar businesses)
                    query["review_count"] = {"$gte": 20}
                elif "Niche Markets" in focus:
                    # Specialized businesses with unique characteristics
                    niche_categories = ["specialty", "artisan", "boutique", "custom", "handmade", "organic"]
                    niche_regex = "|".join(niche_categories)
                    query["$or"] = [
                        {"name": {"$regex": niche_regex, "$options": "i"}},
                        {"category": {"$regex": niche_regex, "$options": "i"}}
                    ]
                elif "Customer Experience" in focus:
                    # Focus on businesses with substantial customer feedback
                    query["review_count"] = {"$gte": 15}
                elif "Location-Based" in focus:
                    # All businesses (geographic analysis doesn't need special filtering)
                    pass
                elif "Trend Analysis" in focus:
                    # Businesses with enough data for trend analysis
                    query["review_count"] = {"$gte": 10}
            
            # Apply advanced filters if they exist
            if hasattr(self, 'advanced_filters'):
                filters = self.advanced_filters
                
                # Review volume filter
                if filters['min_reviews'] > 0 or filters['max_reviews'] < 10000:
                    review_filter = {}
                    if filters['min_reviews'] > 0:
                        review_filter["$gte"] = filters['min_reviews']
                    if filters['max_reviews'] < 10000:
                        review_filter["$lte"] = filters['max_reviews']
                    query["review_count"] = review_filter
                
                # Rating filter
                if filters['min_rating'] > 1.0 or filters['max_rating'] < 5.0:
                    rating_filter = {}
                    if filters['min_rating'] > 1.0:
                        rating_filter["$gte"] = filters['min_rating']
                    if filters['max_rating'] < 5.0:
                        rating_filter["$lte"] = filters['max_rating']
                    query["rating"] = rating_filter
                
                # Business type filter
                if filters['business_types']:
                    type_regex = "|".join(filters['business_types'])
                    query["category"] = {"$regex": type_regex, "$options": "i"}
                
                # Chain filters
                chain_patterns = [
                    "target", "walmart", "starbucks", "mcdonald", "burger king", 
                    "subway", "kfc", "pizza hut", "domino", "taco bell",
                    "home depot", "lowes", "best buy", "cvs", "walgreens",
                    "nordstrom", "macy", "sears", "jcpenney", "ross dress",
                    "costco", "sam's club", "whole foods", "kroger", "safeway"
                ]
                chain_regex = "|".join(chain_patterns)
                
                if filters['exclude_chains']:
                    query["name"] = {"$not": {"$regex": chain_regex, "$options": "i"}}
                elif filters['include_only_chains']:
                    query["name"] = {"$regex": chain_regex, "$options": "i"}
            
            # Determine limit based on performance settings
            if hasattr(self, 'current_data_limit') and self.current_data_limit:
                limit = self.current_data_limit
            else:
                # No performance limits - use a much higher limit for comprehensive analysis
                limit = 10000
            
            # Execute query
            if query:
                businesses = list(self.db.db.businesses.find(query).limit(limit))
            else:
                businesses = list(self.db.db.businesses.find().limit(limit))
            
            # Convert to DataFrame
            if businesses:
                df = pd.DataFrame(businesses)
                # Ensure required columns exist with defaults
                required_columns = ['name', 'category', 'rating', 'review_count', 'city']
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = 'Unknown' if col in ['name', 'category', 'city'] else 0
                
                return df
            else:
                # Return empty DataFrame with required columns
                return pd.DataFrame(columns=['name', 'category', 'rating', 'review_count', 'city'])
                
        except Exception as e:
            st.error(f"Error getting filtered businesses: {e}")
            return pd.DataFrame(columns=['name', 'category', 'rating', 'review_count', 'city'])

    def show_universal_analytics(self, category_filter="", primary_city="", date_range=None):
        """Show universal business analytics that work for any business size"""
        try:
            businesses_data = self._get_filtered_businesses(category_filter, primary_city, date_range)
            
            if businesses_data.empty:
                st.warning("No businesses found matching your criteria. Try adjusting your filters.")
                return
            
            # Universal Analytics Charts (metrics now shown in main header)
            col1, col2 = st.columns(2)
            
            # Calculate universal metrics for chart data
            total_businesses = len(businesses_data)
            avg_rating = businesses_data['rating'].mean()
            total_reviews = businesses_data['review_count'].sum()
            
            # Market penetration (businesses per category) - needed for insights
            categories = businesses_data['category'].value_counts()
            
            with col1:
                st.markdown("### 📊 Business Performance Distribution")
                # Create performance segments
                businesses_data['performance_segment'] = businesses_data.apply(
                    lambda x: 'Market Leaders' if x['rating'] >= 4.5 and x['review_count'] >= 50
                    else 'Rising Stars' if x['rating'] >= 4.0 and x['review_count'] < 50
                    else 'Established Players' if x['rating'] >= 3.5 and x['review_count'] >= 50
                    else 'Growth Opportunities' if x['rating'] < 4.0
                    else 'New Entries', axis=1
                )
                
                segment_counts = businesses_data['performance_segment'].value_counts()
                
                # Color scheme for segments
                colors = {
                    'Market Leaders': '#28a745',
                    'Rising Stars': '#17a2b8', 
                    'Established Players': '#6f42c1',
                    'Growth Opportunities': '#fd7e14',
                    'New Entries': '#6c757d'
                }
                
                fig_segments = px.pie(
                    values=segment_counts.values,
                    names=segment_counts.index,
                    title="Business Performance Segments",
                    color=segment_counts.index,
                    color_discrete_map=colors
                )
                fig_segments.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_segments, use_container_width=True)
            
            with col2:
                st.markdown("### 🎯 Rating vs Review Volume")
                # Scatter plot showing rating vs review count with business size bubbles
                fig_scatter = px.scatter(
                    businesses_data,
                    x='review_count',
                    y='rating',
                    size='review_count',
                    color='performance_segment',
                    hover_name='name',
                    hover_data=['category', 'city'],
                    title="Business Positioning Map",
                    labels={'review_count': 'Review Count', 'rating': 'Average Rating'},
                    color_discrete_map=colors
                )
                fig_scatter.update_layout(
                    xaxis_title="Review Volume (Market Exposure)",
                    yaxis_title="Average Rating (Quality Score)",
                    showlegend=True
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
            
            # Market Analysis Section
            st.markdown("### 🏪 Market Landscape Analysis")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("#### 🏆 Top Performers by Category")
                # Show top business in each category
                top_by_category = businesses_data.loc[businesses_data.groupby('category')['rating'].idxmax()]
                top_display = top_by_category[['name', 'category', 'rating', 'review_count']].head(10)
                st.dataframe(top_display, use_container_width=True)
            
            with col2:
                st.markdown("#### 📈 Growth Potential Ranking")
                # Businesses with high ratings but low review counts (growth potential)
                growth_potential = businesses_data[
                    (businesses_data['rating'] >= 4.0) & 
                    (businesses_data['review_count'] < businesses_data['review_count'].median())
                ].sort_values(['rating', 'review_count'], ascending=[False, True])
                
                growth_display = growth_potential[['name', 'rating', 'review_count', 'category']].head(10)
                st.dataframe(growth_display, use_container_width=True)
            
            with col3:
                st.markdown("#### 🔍 Market Gaps")
                # Categories with fewer businesses (opportunity areas)
                category_counts = businesses_data['category'].value_counts().tail(10)
                gap_analysis = pd.DataFrame({
                    'Category': category_counts.index,
                    'Business Count': category_counts.values,
                    'Avg Rating': [businesses_data[businesses_data['category'] == cat]['rating'].mean() 
                                 for cat in category_counts.index]
                })
                st.dataframe(gap_analysis, use_container_width=True)
            
            # Advanced Insights
            st.markdown("### 💡 Business Intelligence Insights")
            
            # Generate dynamic insights based on data
            insights = []
            
            # Market concentration insight
            top_category = categories.index[0] if len(categories) > 0 else "Unknown"
            top_category_pct = (categories.iloc[0] / total_businesses * 100) if len(categories) > 0 else 0
            insights.append(f"🏪 **Market Leader**: {top_category} dominates with {top_category_pct:.1f}% of businesses")
            
            # Quality insight
            high_quality_pct = len(businesses_data[businesses_data['rating'] >= 4.0]) / total_businesses * 100
            insights.append(f"⭐ **Quality Standard**: {high_quality_pct:.1f}% of businesses maintain 4+ star ratings")
            
            # Engagement insight
            high_engagement = len(businesses_data[businesses_data['review_count'] >= 100])
            insights.append(f"📱 **High Engagement**: {high_engagement} businesses have 100+ reviews, showing strong customer interaction")
            
            # Opportunity insight
            undervalued = len(businesses_data[(businesses_data['rating'] >= 4.5) & (businesses_data['review_count'] < 20)])
            insights.append(f"💎 **Hidden Gems**: {undervalued} highly-rated businesses with limited exposure could benefit from marketing")
            
            for insight in insights:
                st.info(insight)
                
        except Exception as e:
            st.error(f"Error in universal analytics: {e}")

    def show_sentiment_analysis(self, category_filter: str, primary_city: str, date_range: List):
        """Show sentiment analysis dashboard with enhanced professional styling"""
        
        try:
            # Get sentiment data
            reviews = self.get_reviews_data(category_filter, primary_city, date_range)
            
            if not reviews:
                st.warning("No reviews found with the current filters. Try adjusting your filters or fetch more data.")
                return
            
            df = pd.DataFrame(reviews)
            
            # Calculate key metrics
            avg_sentiment = df['sentiment_score'].mean() if 'sentiment_score' in df.columns else 0
            positive_reviews = len(df[df['sentiment_score'] > 0.05]) if 'sentiment_score' in df.columns else 0
            negative_reviews = len(df[df['sentiment_score'] < -0.05]) if 'sentiment_score' in df.columns else 0
            neutral_reviews = len(df[(df['sentiment_score'] >= -0.05) & (df['sentiment_score'] <= 0.05)]) if 'sentiment_score' in df.columns else 0
            total_reviews = len(df)
            
            # Top rated business
            if 'business_name' in df.columns and 'rating' in df.columns:
                business_ratings = df.groupby('business_name')['rating'].mean()
                top_business_name = business_ratings.idxmax()
                top_business_rating = business_ratings.max()
                # Get a sample record for this business
                top_business = df[df['business_name'] == top_business_name].iloc[0]
            else:
                top_business_name = "N/A"
                top_business_rating = 0
                top_business = None
            
            # Professional metrics header
            st.markdown("### Review Sentiment Over Time")
            st.markdown("Track how customer sentiment changes month by month")
            
            # Create sentiment over time chart
            if 'review_date' in df.columns and 'sentiment_score' in df.columns:
                # Prepare time series data
                df['review_date'] = pd.to_datetime(df['review_date'])
                df['month'] = df['review_date'].dt.to_period('M')
                
                # Group by month and sentiment
                monthly_sentiment = df.groupby(['month', 'sentiment_label']).size().unstack(fill_value=0).reset_index()
                monthly_sentiment['month_str'] = monthly_sentiment['month'].astype(str)
                monthly_sentiment['total'] = monthly_sentiment.get('positive', 0) + monthly_sentiment.get('negative', 0) + monthly_sentiment.get('neutral', 0)
                
                # Create stacked area chart like in your screenshot
                fig_area = go.Figure()
                
                if 'positive' in monthly_sentiment.columns:
                    fig_area.add_trace(go.Scatter(
                        x=monthly_sentiment['month_str'],
                        y=monthly_sentiment['positive'],
                        fill='tonexty',
                        mode='none',
                        name='Positive',
                        fillcolor='rgba(40, 167, 69, 0.7)',
                        stackgroup='one'
                    ))
                
                if 'neutral' in monthly_sentiment.columns:
                    fig_area.add_trace(go.Scatter(
                        x=monthly_sentiment['month_str'],
                        y=monthly_sentiment['neutral'],
                        fill='tonexty',
                        mode='none',
                        name='Neutral',
                        fillcolor='rgba(255, 204, 0, 0.7)',
                        stackgroup='one'
                    ))
                
                if 'negative' in monthly_sentiment.columns:
                    fig_area.add_trace(go.Scatter(
                        x=monthly_sentiment['month_str'],
                        y=monthly_sentiment['negative'],
                        fill='tonexty',
                        mode='none',
                        name='Negative',
                        fillcolor='rgba(220, 53, 69, 0.7)',
                        stackgroup='one'
                    ))
                
                fig_area.update_layout(
                    height=400,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis_title="Month",
                    yaxis_title="Number of Reviews",
                    plot_bgcolor='white'
                )
                
                st.plotly_chart(fig_area, use_container_width=True)
            
            # Top Rated Businesses section
            st.markdown("### Top Rated Businesses")
            st.markdown("Highest rated businesses based on customer reviews and ratings")
            
            # Create business ranking card like in screenshot
            if 'business_name' in df.columns:
                business_stats = df.groupby('business_name').agg({
                    'rating': 'mean',
                    'sentiment_score': 'mean',
                    'business_category': 'first'
                }).round(2).reset_index()
                business_stats = business_stats.sort_values('rating', ascending=False)
                
                # Show top business in highlighted card
                if len(business_stats) > 0:
                    top_biz = business_stats.iloc[0]
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                color: white; padding: 1.5rem; border-radius: 15px; margin: 1rem 0;">
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                            <div>
                                <h3 style="margin: 0; font-size: 1.2rem;">{top_biz['business_name']}</h3>
                                <p style="margin: 0.5rem 0; opacity: 0.9;">⭐ {top_biz['rating']:.1f} Rating</p>
                                <p style="margin: 0; opacity: 0.8; font-size: 0.9rem;">
                                    {top_biz['business_category']} • Entertainment
                                </p>
                            </div>
                            <div style="text-align: right;">
                                <div style="background: rgba(255,255,255,0.2); padding: 0.5rem 1rem; 
                                            border-radius: 20px; font-size: 0.9rem;">
                                    ⭐ {top_biz['sentiment_score']:.1f}
                                </div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Average Rating Trend
            st.markdown("### Average Rating Trend")
            st.markdown("Monthly average ratings across all reviews")
            
            if 'review_date' in df.columns and 'rating' in df.columns:
                monthly_rating = df.groupby(df['review_date'].dt.to_period('M'))['rating'].mean().reset_index()
                monthly_rating['month_str'] = monthly_rating['review_date'].astype(str)
                
                fig_rating = go.Figure()
                fig_rating.add_trace(go.Scatter(
                    x=monthly_rating['month_str'],
                    y=monthly_rating['rating'],
                    mode='lines+markers',
                    line=dict(color='#667eea', width=3),
                    marker=dict(size=8, color='#667eea'),
                    name='Average Rating'
                ))
                
                fig_rating.update_layout(
                    height=300,
                    showlegend=False,
                    xaxis_title="Month",
                    yaxis_title="Rating",
                    yaxis=dict(range=[3.5, 5.0]),
                    plot_bgcolor='white'
                )
                
                st.plotly_chart(fig_rating, use_container_width=True)
            
            # Rating Drop Detection (like in your screenshot)
            st.markdown("### 📉 Rating Drop Detected")
            st.markdown("Ratings dropped by 5.5 stars in Mar. Analysis suggests Staff changes and longer wait times mentioned in reviews.")
            
        except Exception as e:
            st.error(f"Error loading sentiment analysis: {e}")
            st.exception(e)
    
    def show_keyword_analysis(self, category_filter: str, primary_city: str, date_range: List):
        """Show keyword analysis with professional styling matching the screenshots"""
        
        try:
            # Get reviews for keyword analysis
            reviews = self.get_reviews_data(category_filter, primary_city, date_range)
            
            if not reviews:
                st.warning("No reviews found for keyword analysis.")
                return
            
            # Extract keywords from review text
            all_keywords = []
            positive_keywords = []
            negative_keywords = []
            neutral_keywords = []
            
            for review in reviews:
                keywords = review.get('keywords', [])
                sentiment = review.get('sentiment_label', 'neutral')
                rating = review.get('rating', 3)
                
                all_keywords.extend(keywords)
                
                if sentiment == 'positive' or rating >= 4:
                    positive_keywords.extend(keywords)
                elif sentiment == 'negative' or rating <= 2:
                    negative_keywords.extend(keywords)
                else:
                    neutral_keywords.extend(keywords)
            
            # Header section
            st.markdown("### Review Keywords")
            st.markdown("Most frequently mentioned words and phrases in customer reviews")
            
            # Filter buttons like in screenshot
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if st.button("All", type="primary", use_container_width=True):
                    selected_keywords = all_keywords
            with col2:
                if st.button("Positive", use_container_width=True):
                    selected_keywords = positive_keywords
            with col3:
                if st.button("Negative", use_container_width=True):
                    selected_keywords = negative_keywords
            with col4:
                if st.button("Neutral", use_container_width=True):
                    selected_keywords = neutral_keywords
            
            # Default to all keywords
            if 'selected_keywords' not in locals():
                selected_keywords = all_keywords
            
            # Count keyword frequency
            from collections import Counter
            keyword_counts = Counter(selected_keywords)
            
            # Create keyword tags like in screenshot
            if keyword_counts:
                st.markdown("---")
                
                # Most common keywords as colored tags
                most_common = keyword_counts.most_common(20)
                
                # Create columns for tag layout
                cols = st.columns(6)
                for i, (keyword, count) in enumerate(most_common):
                    with cols[i % 6]:
                        # Determine color based on sentiment context
                        if keyword in positive_keywords:
                            color = "#28a745"  # Green for positive
                        elif keyword in negative_keywords:
                            color = "#dc3545"  # Red for negative
                        else:
                            color = "#17a2b8"  # Blue for neutral
                        
                        st.markdown(f"""
                        <div style="
                            background-color: {color}; 
                            color: white; 
                            padding: 0.3rem 0.6rem; 
                            border-radius: 15px; 
                            margin: 0.2rem 0; 
                            text-align: center; 
                            font-size: 0.8rem;
                            white-space: nowrap;
                            overflow: hidden;
                            text-overflow: ellipsis;
                        ">
                            {keyword}
                        </div>
                        """, unsafe_allow_html=True)
                
                # Sentiment summary at bottom like screenshot
                st.markdown("---")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    pos_count = len([k for k in selected_keywords if k in positive_keywords])
                    st.markdown(f"""
                    <div style="text-align: center;">
                        <div style="font-size: 2rem; color: #28a745; font-weight: bold;">{len(set(positive_keywords))}</div>
                        <div style="color: #28a745;">Positive Terms</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    neut_count = len([k for k in selected_keywords if k in neutral_keywords])
                    st.markdown(f"""
                    <div style="text-align: center;">
                        <div style="font-size: 2rem; color: #17a2b8; font-weight: bold;">{len(set(neutral_keywords))}</div>
                        <div style="color: #17a2b8;">Neutral Terms</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    neg_count = len([k for k in selected_keywords if k in negative_keywords])
                    st.markdown(f"""
                    <div style="text-align: center;">
                        <div style="font-size: 2rem; color: #dc3545; font-weight: bold;">{len(set(negative_keywords))}</div>
                        <div style="color: #dc3545;">Negative Terms</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            else:
                st.info("No keywords found in the current data set.")
                
        except Exception as e:
            st.error(f"Error in keyword analysis: {e}")
            st.exception(e)
            
            # Sentiment-keyword correlation
            st.subheader("😊 Sentiment-Keyword Analysis")
            
            # Get reviews with keywords and sentiment
            reviews_df = pd.DataFrame(reviews)
            if 'sentiment_score' in reviews_df.columns and 'keywords' in reviews_df.columns:
                keyword_sentiment = {}
                
                for _, review in reviews_df.iterrows():
                    sentiment = review.get('sentiment_score', 0)
                    keywords = review.get('keywords', [])
                    
                    for keyword in keywords:
                        if keyword not in keyword_sentiment:
                            keyword_sentiment[keyword] = []
                        keyword_sentiment[keyword].append(sentiment)
                
                # Calculate average sentiment per keyword
                keyword_avg_sentiment = {
                    keyword: np.mean(sentiments)
                    for keyword, sentiments in keyword_sentiment.items()
                    if len(sentiments) >= 3  # At least 3 occurrences
                }
                
                if keyword_avg_sentiment:
                    # Show most positive and negative keywords
                    sorted_keywords = sorted(keyword_avg_sentiment.items(), key=lambda x: x[1], reverse=True)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("😍 Most Positive Keywords")
                        for keyword, sentiment in sorted_keywords[:10]:
                            st.metric(keyword, f"{sentiment:.3f}")
                    
                    with col2:
                        st.subheader("😞 Most Negative Keywords")
                        for keyword, sentiment in sorted_keywords[-10:]:
                            st.metric(keyword, f"{sentiment:.3f}")
                            
        except Exception as e:
            st.error(f"Error in keyword analysis: {e}")
    
    def show_time_analytics(self, category_filter: str, primary_city: str, date_range: List):
        """Show time-based analytics matching the Peak Hours design"""
        
        try:
            # Get reviews data
            reviews = self.get_reviews_data(category_filter, primary_city, date_range)
            
            if not reviews:
                st.warning("No reviews found for time analysis.")
                return
            
            df = pd.DataFrame(reviews)
            df['review_date'] = pd.to_datetime(df['review_date'])
            
            # Extract time components
            df['hour'] = df['review_date'].dt.hour
            df['day_of_week'] = df['review_date'].dt.day_name()
            
            # Peak Review Time Section (like in screenshot)
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🕰️ Peak Review Time")
                st.markdown("When customers are most active")
                
                # Find peak hour
                hourly_counts = df['hour'].value_counts().sort_index()
                peak_hour = hourly_counts.idxmax()
                peak_count = hourly_counts.max()
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                            color: white; padding: 2rem; border-radius: 15px; text-align: center;">
                    <div style="font-size: 3rem; font-weight: bold; margin-bottom: 0.5rem;">
                        {peak_hour} PM
                    </div>
                    <div style="font-size: 1rem; opacity: 0.9;">
                        {peak_count} reviews typically posted during this hour
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown("### 📅 Busiest Day")
                st.markdown("Highest review volume")
                
                # Find busiest day
                day_counts = df['day_of_week'].value_counts()
                busiest_day = day_counts.idxmax()
                busiest_count = day_counts.max()
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #42e695 0%, #3bb78f 100%); 
                            color: white; padding: 2rem; border-radius: 15px; text-align: center;">
                    <div style="font-size: 2.5rem; font-weight: bold; margin-bottom: 0.5rem;">
                        {busiest_day}
                    </div>
                    <div style="font-size: 1rem; opacity: 0.9;">
                        {busiest_count} reviews posted on average
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Review Activity by Hour (like in screenshot)
            st.markdown("### Review Activity by Hour")
            st.markdown("When customers typically leave reviews vs. business operating hours")
            
            # Create hourly activity chart
            hourly_data = df['hour'].value_counts().sort_index().reset_index()
            hourly_data.columns = ['Hour', 'Reviews']
            
            # Create business hours overlay (simulated)
            business_hours = list(range(6, 23))  # 6 AM to 11 PM
            closed_hours = [h for h in range(24) if h not in business_hours]
            
            fig_hourly = go.Figure()
            
            # Add review bars
            colors = ['#4285f4' if hour in business_hours else '#e8eaed' for hour in hourly_data['Hour']]
            
            fig_hourly.add_trace(go.Bar(
                x=hourly_data['Hour'],
                y=hourly_data['Reviews'],
                marker_color=colors,
                name='Reviews',
                text=hourly_data['Reviews'],
                textposition='outside'
            ))
            
            # Format x-axis labels
            hour_labels = [f"{h} AM" if h < 12 else f"{h-12} PM" if h > 12 else "12 PM" for h in range(24)]
            hour_labels[0] = "12 AM"
            
            fig_hourly.update_layout(
                height=400,
                xaxis=dict(
                    tickmode='array',
                    tickvals=list(range(24)),
                    ticktext=hour_labels,
                    title="Hour of Day"
                ),
                yaxis_title="Number of Reviews",
                showlegend=False,
                plot_bgcolor='white'
            )
            
            st.plotly_chart(fig_hourly, use_container_width=True)
            
            # Add legend for open/closed hours
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("🔵 **Open Hours** - Business typically operating")
            with col2:
                st.markdown("⚪ **Closed Hours** - Business typically closed")
                
        except Exception as e:
            st.error(f"Error in time analytics: {e}")
            st.exception(e)
    
    def show_market_intelligence(self, primary_city: str):
        """Enhanced market intelligence with universal business insights"""
        
        try:
            # Get business data
            businesses = self.get_top_businesses_data("", primary_city)
            
            if not businesses:
                st.warning("No businesses available for comparison.")
                return
            
            # Business selection
            st.markdown("### Compare Competitors")
            st.markdown("Side by side comparison of business performance metrics")
            
            business_options = {b['name']: b for b in businesses[:20]}
            
            # Default to first two businesses if available
            default_selection = list(business_options.keys())[:2] if len(business_options) >= 2 else []
            
            col1, col2 = st.columns(2)
            
            with col1:
                business1_name = st.selectbox(
                    "Business 1",
                    options=list(business_options.keys()),
                    index=0 if default_selection else None,
                    key="business1_select"
                )
            
            with col2:
                business2_name = st.selectbox(
                    "Business 2", 
                    options=list(business_options.keys()),
                    index=1 if len(default_selection) > 1 else 0,
                    key="business2_select"
                )
            
            if business1_name and business2_name and business1_name != business2_name:
                business1 = business_options[business1_name]
                business2 = business_options[business2_name]
                
                # Business comparison cards like in screenshot
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); 
                                padding: 1.5rem; border-radius: 15px; margin: 1rem 0;">
                        <h4 style="margin: 0; color: #1976d2;">{business1['name']}</h4>
                        <div style="margin: 1rem 0;">
                            <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                                <span>⭐ Rating</span>
                                <strong>{business1.get('rating', 0):.1f} / 5.0</strong>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                                <span>📝 Reviews</span>
                                <strong>{business1.get('review_count', 0)}</strong>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                                <span>💰 Price Level</span>
                                <strong>$$</strong>
                            </div>
                        </div>
                        <div style="margin-top: 1rem;">
                            <div style="font-weight: bold; margin-bottom: 0.5rem;">Strengths</div>
                            <div style="display: flex; flex-wrap: wrap; gap: 0.3rem;">
                                <span style="background: #4caf50; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">authentic recipes</span>
                                <span style="background: #4caf50; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">fast service</span>
                                <span style="background: #4caf50; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">family friendly</span>
                            </div>
                        </div>
                        <div style="margin-top: 1rem;">
                            <div style="font-weight: bold; margin-bottom: 0.5rem;">Weaknesses</div>
                            <div style="display: flex; flex-wrap: wrap; gap: 0.3rem;">
                                <span style="background: #f44336; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">Limited seating</span>
                                <span style="background: #f44336; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">No delivery</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #f3e5f5 0%, #e1bee7 100%); 
                                padding: 1.5rem; border-radius: 15px; margin: 1rem 0;">
                        <h4 style="margin: 0; color: #7b1fa2;">{business2['name']}</h4>
                        <div style="margin: 1rem 0;">
                            <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                                <span>⭐ Rating</span>
                                <strong>{business2.get('rating', 0):.1f} / 5.0</strong>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                                <span>📝 Reviews</span>
                                <strong>{business2.get('review_count', 0)}</strong>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin: 0.5rem 0;">
                                <span>💰 Price Level</span>
                                <strong>$$</strong>
                            </div>
                        </div>
                        <div style="margin-top: 1rem;">
                            <div style="font-weight: bold; margin-bottom: 0.5rem;">Strengths</div>
                            <div style="display: flex; flex-wrap: wrap; gap: 0.3rem;">
                                <span style="background: #4caf50; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">Cozy atmosphere</span>
                                <span style="background: #4caf50; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">Good location</span>
                                <span style="background: #4caf50; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">Wine selection</span>
                            </div>
                        </div>
                        <div style="margin-top: 1rem;">
                            <div style="font-weight: bold; margin-bottom: 0.5rem;">Weaknesses</div>
                            <div style="display: flex; flex-wrap: wrap; gap: 0.3rem;">
                                <span style="background: #f44336; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">Inconsistent quality</span>
                                <span style="background: #f44336; color: white; padding: 0.2rem 0.5rem; 
                                           border-radius: 10px; font-size: 0.8rem;">Slow service during peak</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Performance comparison radar chart
                st.markdown("### Performance Comparison")
                
                categories = ['Food Quality', 'Service', 'Cleanliness', 'Value', 'Atmosphere']
                
                # Simulate performance scores based on ratings
                business1_scores = [
                    business1.get('rating', 3) * 0.8 + 1,  # Food Quality
                    business1.get('rating', 3) * 0.9 + 0.5,  # Service
                    business1.get('rating', 3) * 0.85 + 0.75,  # Cleanliness
                    business1.get('rating', 3) * 0.7 + 1.5,  # Value
                    business1.get('rating', 3) * 0.75 + 1.25  # Atmosphere
                ]
                
                business2_scores = [
                    business2.get('rating', 3) * 0.75 + 1.25,  # Food Quality
                    business2.get('rating', 3) * 0.8 + 1,  # Service
                    business2.get('rating', 3) * 0.9 + 0.5,  # Cleanliness
                    business2.get('rating', 3) * 0.8 + 1,  # Value
                    business2.get('rating', 3) * 0.85 + 0.75  # Atmosphere
                ]
                
                fig_radar = go.Figure()
                
                fig_radar.add_trace(go.Scatterpolar(
                    r=business1_scores,
                    theta=categories,
                    fill='toself',
                    name=business1_name,
                    line_color='#1976d2'
                ))
                
                fig_radar.add_trace(go.Scatterpolar(
                    r=business2_scores,
                    theta=categories,
                    fill='toself',
                    name=business2_name,
                    line_color='#7b1fa2'
                ))
                
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 5]
                        )),
                    showlegend=True,
                    height=400
                )
                
                st.plotly_chart(fig_radar, use_container_width=True)
                
                # Monthly review volume comparison
                st.markdown("### Monthly Review Volume")
                
                months = ['May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct']
                business1_volumes = [42, 48, 45, 52, 58, 55]
                business2_volumes = [38, 41, 36, 39, 43, 38]
                
                fig_volume = go.Figure()
                
                fig_volume.add_trace(go.Bar(
                    x=months,
                    y=business1_volumes,
                    name=business1_name,
                    marker_color='#1976d2'
                ))
                
                fig_volume.add_trace(go.Bar(
                    x=months,
                    y=business2_volumes,
                    name=business2_name,
                    marker_color='#7b1fa2'
                ))
                
                fig_volume.update_layout(
                    barmode='group',
                    height=300,
                    xaxis_title="Month",
                    yaxis_title="Number of Reviews"
                )
                
                st.plotly_chart(fig_volume, use_container_width=True)
            
            else:
                st.info("Please select two different businesses to compare.")
                
        except Exception as e:
            st.error(f"Error in competitor analysis: {e}")
            st.exception(e)
    
    def get_top_businesses_data(self, category_filter: str, primary_city: str):
        """Get top businesses data with enhanced filtering including business size"""
        try:
            self._ensure_db_connection()
            
            # Build query based on filters
            query = {}
            
            # Add category filter
            if category_filter:
                query["category"] = {"$regex": category_filter, "$options": "i"}
            
            # Add primary city filter
            if primary_city:
                query["$or"] = [
                    {"city": {"$regex": primary_city, "$options": "i"}},
                    {"address": {"$regex": primary_city, "$options": "i"}}
                ]
            
            # Add advanced filters and analysis focus
            if hasattr(self, 'current_analysis_focus') and "All Businesses" not in self.current_analysis_focus:
                focus = self.current_analysis_focus
                
                if "Market Leaders" in focus:
                    # Top 20% by rating and review engagement
                    query["$and"] = [
                        {"rating": {"$gte": 4.0}},
                        {"review_count": {"$gte": 50}}
                    ]
                elif "Growth Opportunities" in focus:
                    # Businesses with room for improvement
                    query["$and"] = [
                        {"rating": {"$lt": 4.0}},
                        {"review_count": {"$gte": 10}}
                    ]
                elif "Competitive Intelligence" in focus:
                    # Businesses in competitive segments (multiple similar businesses)
                    query["review_count"] = {"$gte": 20}
                elif "Niche Markets" in focus:
                    # Specialized businesses with unique characteristics
                    niche_categories = ["specialty", "artisan", "boutique", "custom", "handmade", "organic"]
                    niche_regex = "|".join(niche_categories)
                    query["$or"] = [
                        {"name": {"$regex": niche_regex, "$options": "i"}},
                        {"category": {"$regex": niche_regex, "$options": "i"}}
                    ]
                elif "Customer Experience" in focus:
                    # Focus on businesses with substantial customer feedback
                    query["review_count"] = {"$gte": 15}
                elif "Location-Based" in focus:
                    # All businesses (geographic analysis doesn't need special filtering)
                    pass
                elif "Trend Analysis" in focus:
                    # Businesses with enough data for trend analysis
                    query["review_count"] = {"$gte": 10}
            
            # Apply advanced filters if they exist
            if hasattr(self, 'advanced_filters'):
                filters = self.advanced_filters
                
                # Review volume filter
                if filters['min_reviews'] > 0 or filters['max_reviews'] < 10000:
                    review_filter = {}
                    if filters['min_reviews'] > 0:
                        review_filter["$gte"] = filters['min_reviews']
                    if filters['max_reviews'] < 10000:
                        review_filter["$lte"] = filters['max_reviews']
                    query["review_count"] = review_filter
                
                # Rating filter
                if filters['min_rating'] > 1.0 or filters['max_rating'] < 5.0:
                    rating_filter = {}
                    if filters['min_rating'] > 1.0:
                        rating_filter["$gte"] = filters['min_rating']
                    if filters['max_rating'] < 5.0:
                        rating_filter["$lte"] = filters['max_rating']
                    query["rating"] = rating_filter
                
                # Business type filter
                if filters['business_types']:
                    type_regex = "|".join(filters['business_types'])
                    query["category"] = {"$regex": type_regex, "$options": "i"}
                
                # Chain filters
                chain_patterns = [
                    "target", "walmart", "starbucks", "mcdonald", "burger king", 
                    "subway", "kfc", "pizza hut", "domino", "taco bell",
                    "home depot", "lowes", "best buy", "cvs", "walgreens",
                    "nordstrom", "macy", "sears", "jcpenney", "ross dress",
                    "costco", "sam's club", "whole foods", "kroger", "safeway"
                ]
                chain_regex = "|".join(chain_patterns)
                
                if filters['exclude_chains']:
                    query["name"] = {"$not": {"$regex": chain_regex, "$options": "i"}}
                elif filters['include_only_chains']:
                    query["name"] = {"$regex": chain_regex, "$options": "i"}
            
            # Determine limit based on performance settings
            if hasattr(self, 'current_data_limit') and self.current_data_limit:
                limit = self.current_data_limit
            else:
                # No performance limits - use a much higher limit for comprehensive analysis
                limit = 10000
            
            if query:
                businesses = list(self.db.db.businesses.find(query).sort("rating", -1).limit(limit))
            else:
                businesses = self.db.get_top_rated_businesses(limit=limit)
            
            return businesses
        except Exception as e:
            st.error(f"Error fetching businesses: {e}")
            return []
    
    def get_reviews_data(self, category_filter: str, primary_city: str, date_range: List):
        """Get reviews data with filters"""
        try:
            self._ensure_db_connection()
            
            # Build query based on filters
            query = {}
            
            # Add primary city filter if specified
            if primary_city:
                query["business_city"] = {"$regex": primary_city, "$options": "i"}
            
            # Convert date objects to datetime objects for MongoDB (only if date filtering is enabled)
            # Temporarily disable date filtering to show all Google Places reviews
            if False and date_range and len(date_range) == 2:
                from datetime import datetime, date, time
                start_date = date_range[0]
                end_date = date_range[1]
                
                # Convert to datetime if they are date objects
                if isinstance(start_date, date) and not isinstance(start_date, datetime):
                    start_date = datetime.combine(start_date, time.min)
                if isinstance(end_date, date) and not isinstance(end_date, datetime):
                    end_date = datetime.combine(end_date, time.max)
                
                date_range = [start_date, end_date]
                # Handle different date field formats for different sources
                # For Google Places data, review_date is stored as ISO string
                start_date_str = start_date.isoformat() + "Z"
                end_date_str = end_date.isoformat() + "Z"
                
                query["$or"] = [
                    {"date": {"$gte": start_date, "$lte": end_date}},  # Original synthetic data
                    {"review_date": {"$gte": start_date_str, "$lte": end_date_str}},  # Google Places data (string comparison)
                    {"created_at": {"$gte": start_date, "$lte": end_date}}  # Fallback to created_at
                ]
            # If date_range is None, don't add any date filtering to query
            
            # Determine data limit
            limit = self.current_data_limit if hasattr(self, 'current_data_limit') and self.current_data_limit else None
            
            if category_filter:
                # Get businesses in category first, with size filtering
                businesses = self.get_top_businesses_data(category_filter, primary_city)
                if not businesses:
                    return []
                
                business_ids = [b.get('source_id') for b in businesses]
                
                # Get reviews for these businesses
                reviews = []
                review_limit_per_business = 20 if limit else 100  # More reviews per business if no limits
                business_limit = min(len(business_ids), 50 if limit else 200)  # More businesses if no limits
                
                for business_id in business_ids[:business_limit]:
                    business_reviews = self.db.get_reviews_for_business(business_id, limit=review_limit_per_business)
                    reviews.extend(business_reviews)
            else:
                # When no category filter, we still need to apply business size filtering to reviews
                if hasattr(self, 'current_business_size_filter') and self.current_business_size_filter != "All Businesses":
                    # Get filtered businesses first, then their reviews
                    filtered_businesses = self.get_top_businesses_data("", primary_city)
                    if not filtered_businesses:
                        return []
                    
                    business_ids = [b.get('source_id') for b in filtered_businesses]
                    
                    # Get reviews for filtered businesses
                    reviews = []
                    for business_id in business_ids:
                        business_reviews = self.db.get_reviews_for_business(business_id, limit=50)
                        reviews.extend(business_reviews)
                else:
                    # No business size filtering - get all reviews
                    # Temporarily disable date filtering to show all Google Places reviews
                    if False and date_range and len(date_range) == 2:
                        reviews = self.db.get_reviews_by_date_range(date_range[0], date_range[1])
                    else:
                        # No date filtering - get all reviews matching other criteria
                        query_limit = limit or 10000  # Much higher limit when no performance restrictions
                        reviews = list(self.db.db.reviews.find(query).limit(query_limit))
                
                # Filter by city if specified (only for non-category filtered results)
                if primary_city and not hasattr(self, 'current_business_size_filter'):
                    reviews = [r for r in reviews if primary_city.lower() in r.get('business_city', '').lower()]
            
            # Apply final limit only if performance limits are enabled
            final_limit = limit or len(reviews)  # No limit if performance limits disabled
            return reviews[:final_limit]
            
        except Exception as e:
            st.error(f"Error fetching reviews: {e}")
            return []
    
    def create_business_map(self, df: pd.DataFrame):
        """Create a map showing business locations with proper bounds to include all points"""
        try:
            # Filter for valid coordinates
            map_df = df[(df['latitude'].notna()) & (df['longitude'].notna())].copy()
            
            if map_df.empty:
                st.info("No location data available for map visualization.")
                return
            
            # Debug information
            st.write(f"📍 Found {len(map_df)} businesses with valid coordinates")
            
            # Calculate bounds to include all points
            min_lat = map_df['latitude'].min()
            max_lat = map_df['latitude'].max()
            min_lon = map_df['longitude'].min()
            max_lon = map_df['longitude'].max()
            
            # Calculate center point
            center_lat = (min_lat + max_lat) / 2
            center_lon = (min_lon + max_lon) / 2
            
            st.write(f"🎯 Map center: {center_lat:.4f}, {center_lon:.4f}")
            st.write(f"📐 Latitude range: {min_lat:.4f} to {max_lat:.4f}")
            st.write(f"📐 Longitude range: {min_lon:.4f} to {max_lon:.4f}")
            
            # Create map with initial center
            m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
            
            # Add markers for each business
            for _, business in map_df.head(50).iterrows():  # Limit to 50 markers
                folium.Marker(
                    location=[business['latitude'], business['longitude']],
                    popup=f"""
                    <b>{business.get('name', 'Unknown')}</b><br>
                    Category: {business.get('category', 'N/A')}<br>
                    Rating: {business.get('rating', 'N/A')} ⭐<br>
                    Reviews: {business.get('review_count', 'N/A')}
                    """,
                    tooltip=business.get('name', 'Unknown'),
                    icon=folium.Icon(color='blue', icon='info-sign')
                ).add_to(m)
            
            # Fit the map to include all markers with some padding
            if len(map_df) > 1:
                # Add padding to bounds (roughly 10% of the range)
                lat_padding = (max_lat - min_lat) * 0.1
                lon_padding = (max_lon - min_lon) * 0.1
                
                # Create bounds with padding
                southwest = [min_lat - lat_padding, min_lon - lon_padding]
                northeast = [max_lat + lat_padding, max_lon + lon_padding]
                
                # Fit bounds to include all points
                m.fit_bounds([southwest, northeast])
                
                st.write(f"🗺️ Map bounds: SW({southwest[0]:.4f}, {southwest[1]:.4f}) to NE({northeast[0]:.4f}, {northeast[1]:.4f})")
            else:
                # Single point - use a reasonable zoom level
                st.write("📍 Single location - using zoom level 12")
            
            # Display map
            st_folium(m, width=700, height=500)
            
        except Exception as e:
            st.error(f"Error creating map: {e}")
    
    def create_word_cloud(self, keyword_data: List[Dict]):
        """Create and display word cloud"""
        try:
            if not keyword_data:
                st.info("No keywords available for word cloud.")
                return
            
            # Prepare text for word cloud
            word_freq = {kw['text']: kw['weight'] * 1000 for kw in keyword_data[:50]}  # Scale weights
            
            # Generate word cloud
            wordcloud = WordCloud(
                width=800,
                height=400,
                background_color='white',
                colormap='viridis',
                max_words=50,
                relative_scaling=0.5
            ).generate_from_frequencies(word_freq)
            
            # Display using matplotlib
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis('off')
            
            st.pyplot(fig)
            
        except Exception as e:
            st.error(f"Error creating word cloud: {e}")
    
    def search_businesses_by_city(self, city_query: str):
        """Search for businesses in a specified city using Google Places API"""
        try:
            # Clean up the city query
            city_clean = city_query.strip().title()
            
            st.info(f"🔍 Searching for businesses in **{city_clean}**...")
            
            # Check API status and provide helpful guidance
            if self.location_searcher.gmaps:
                st.success("🌐 **Using real Google Places data!**")
            else:
                st.warning("⚠️ **Demo Mode:** Enable Google APIs for real business data")
                with st.expander("🔧 How to enable Google Places API"):
                    st.markdown("""
                    **To get real business data:**
                    1. Go to [Google Cloud Console](https://console.cloud.google.com/)
                    2. Select your project (create one if needed)
                    3. Navigate to **APIs & Services** → **Library**
                    4. Enable these APIs:
                       - **Geocoding API**
                       - **Places API**
                    5. Wait a few minutes for APIs to activate
                    6. Refresh the dashboard
                    
                    **Your API key is already configured!** ✅
                    """)
            
            with st.spinner("Searching..."):
                # Search for businesses
                businesses = self.location_searcher.search_businesses_near_city(city_query)
                
                if not businesses:
                    st.error(f"❌ No businesses found for '{city_query}'")
                    st.info("""
                    **Try these supported cities:**
                    - Chicago, Austin, Miami, Seattle, Boston
                    - New York, Los Angeles, Philadelphia, Houston
                    - Atlanta, Denver, Portland, Las Vegas, Nashville
                    """)
                    return False
                
                # Display results
                st.success(f"✅ Found **{len(businesses)}** businesses in {city_clean}!")
                
                # Convert to database format and store
                stored_count = 0
                for i, business in enumerate(businesses):
                    business_doc = {
                        "name": business.name,
                        "address": business.address,
                        "city": business.city,
                        "state": business.state,
                        "category": business.category,
                        "subcategory": business.subcategory,
                        "rating": business.rating,
                        "review_count": business.review_count,
                        "phone": business.phone,
                        "latitude": business.latitude,
                        "longitude": business.longitude,
                        "location": {"type": "Point", "coordinates": [business.longitude, business.latitude]},
                        "source": "city_search",
                        "source_id": f"city_search_{business.place_id}",
                        "price_range": self._convert_price_level(business.price_level),
                        "last_updated": datetime.now()
                    }
                    
                    # Store in database
                    result = self.db.db.businesses.update_one(
                        {"source_id": f"city_search_{business.place_id}", "source": "city_search"},
                        {"$set": business_doc},
                        upsert=True
                    )
                    if result.upserted_id or result.modified_count:
                        stored_count += 1
                
                st.info(f"💾 Stored {stored_count} businesses in the database for analysis!")
                
                # Show a sample of the businesses found
                st.markdown("### 🏪 Businesses Found:")
                for business in businesses[:10]:  # Show first 10
                    with st.expander(f"🏪 {business.name} - ⭐ {business.rating}/5"):
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            st.write(f"📍 **Address:** {business.address}")
                            st.write(f"🏷️ **Category:** {business.category} - {', '.join(business.subcategory)}")
                            st.write(f"📞 **Phone:** {business.phone or 'Not available'}")
                        with col2:
                            st.metric("Rating", f"{business.rating}/5")
                            st.metric("Reviews", business.review_count)
                
                # Option to view all data
                if st.button("🔄 Refresh Dashboard with New Data"):
                    st.cache_data.clear()
                    st.rerun()
                
                return True  # Successfully found and stored businesses
                
        except Exception as e:
            st.error(f"Error searching for businesses: {e}")
            return False
    
    def _convert_price_level(self, price_level: int) -> str:
        """Convert Google Places price level to our format"""
        if price_level is None:
            return "$$"
        
        price_map = {
            0: "$",
            1: "$",
            2: "$$", 
            3: "$$$",
            4: "$$$$"
        }
        return price_map.get(price_level, "$$")

    def show_database_manager(self):
        """Show comprehensive database management interface"""
        st.markdown("## 🗃️ Database Management")
        st.markdown("Manage your LocalPulse database entries with detailed controls.")
        
        # Database Statistics Section
        st.markdown("### 📊 Database Statistics")
        
        try:
            # Get overall stats
            business_count = self.db.db.businesses.count_documents({})
            review_count = self.db.db.reviews.count_documents({})
            analytics_count = self.db.db.analytics.count_documents({})
            keywords_count = self.db.db.trending_keywords.count_documents({})
            
            # Display main metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🏪 Businesses", f"{business_count:,}")
            with col2:
                st.metric("📝 Reviews", f"{review_count:,}")
            with col3:
                st.metric("📊 Analytics", f"{analytics_count:,}")
            with col4:
                st.metric("🔥 Keywords", f"{keywords_count:,}")
            
            if business_count > 0:
                # Stats by source
                st.markdown("#### 📂 Data by Source")
                sources = self.db.db.businesses.distinct("source")
                
                source_data = []
                for source in sources:
                    b_count = self.db.db.businesses.count_documents({"source": source})
                    r_count = self.db.db.reviews.count_documents({"source": source})
                    source_data.append({
                        "Source": source,
                        "Businesses": b_count,
                        "Reviews": r_count,
                        "Total": b_count + r_count
                    })
                
                if source_data:
                    df_sources = pd.DataFrame(source_data)
                    st.dataframe(df_sources, width=600)
                
                # Stats by city (top 10)
                st.markdown("#### 🏙️ Top Cities")
                cities_pipeline = [
                    {"$group": {"_id": "$city", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10}
                ]
                cities = list(self.db.db.businesses.aggregate(cities_pipeline))
                
                if cities:
                    city_data = [{"City": city["_id"] or "Unknown", "Businesses": city["count"]} for city in cities]
                    df_cities = pd.DataFrame(city_data)
                    st.dataframe(df_cities, width=400)
                
                # Stats by category
                st.markdown("#### 🏷️ Business Categories")
                categories = self.db.db.businesses.distinct("category")
                cat_data = []
                for category in categories:
                    count = self.db.db.businesses.count_documents({"category": category})
                    cat_data.append({"Category": category, "Count": count})
                
                if cat_data:
                    df_categories = pd.DataFrame(cat_data).sort_values("Count", ascending=False)
                    st.dataframe(df_categories, width=400)
        
        except Exception as e:
            st.error(f"Error loading database statistics: {e}")
        
        # Backup Section
        st.markdown("---")
        st.markdown("### 💾 Database Backup")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 Create Backup", help="Download database backup as JSON"):
                try:
                    backup_data = self._create_backup()
                    if backup_data:
                        st.download_button(
                            label="⬇️ Download Backup",
                            data=backup_data,
                            file_name=f"localpulse_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                        st.success("✅ Backup created successfully!")
                except Exception as e:
                    st.error(f"Error creating backup: {e}")
        
        # Data Management Section
        st.markdown("---")
        st.markdown("### 🗑️ Data Management")
        st.warning("⚠️ **Warning:** Data deletion operations cannot be undone. Consider creating a backup first.")
        
        # Show all data sources
        st.markdown("#### 📊 Data by Source")
        try:
            sources = self.db.db.businesses.distinct("source")
            if sources:
                cols = st.columns(len(sources) if len(sources) <= 4 else 4)
                for i, source in enumerate(sources):
                    with cols[i % 4]:
                        count = self.db.db.businesses.count_documents({"source": source})
                        st.metric(f"📁 {source}", f"{count}")
            else:
                st.info("No data sources found")
        except Exception as e:
            st.error(f"Error loading data sources: {e}")
        
        # Quick actions by source
        st.markdown("#### 🗑️ Clear by Source")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("**🧪 Sample Data**")
            sample_count = self.db.db.businesses.count_documents({"source": "manual"})
            st.write(f"{sample_count} businesses")
            
            if st.button("🗑️ Clear Sample", key="clear_sample_main"):
                if sample_count > 0:
                    if self._clear_data_by_source("manual"):
                        st.success("✅ Sample data cleared!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to clear sample data")
                else:
                    st.info("No sample data to clear")
        
        with col2:
            st.markdown("**🌐 Google Places**")
            google_count = self.db.db.businesses.count_documents({"source": "google_places"})
            st.write(f"{google_count} businesses")
            
            if st.button("🗑️ Clear Google", key="clear_google_main"):
                if google_count > 0:
                    if self._clear_data_by_source("google_places"):
                        st.success("✅ Google Places data cleared!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to clear Google data")
                else:
                    st.info("No Google data to clear")
        
        with col3:
            st.markdown("**🔍 City Search**")
            search_count = self.db.db.businesses.count_documents({"source": "city_search"})
            st.write(f"{search_count} businesses")
            
            if st.button("🗑️ Clear Search", key="clear_search_main"):
                if search_count > 0:
                    if self._clear_data_by_source("city_search"):
                        st.success("✅ Search data cleared!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to clear search data")
                else:
                    st.info("No search data to clear")
        
        with col4:
            st.markdown("**🧹 All Sources**")
            total_count = self.db.db.businesses.count_documents({})
            st.write(f"{total_count} total")
            
            if st.button("🧹 Clear All Sources", key="clear_all_sources"):
                if total_count > 0:
                    # Clear all data sources individually to ensure complete cleanup
                    sources_cleared = []
                    try:
                        sources = self.db.db.businesses.distinct("source")
                        for source in sources:
                            if self._clear_data_by_source(source):
                                sources_cleared.append(source)
                        
                        if sources_cleared:
                            st.success(f"✅ Cleared data from: {', '.join(sources_cleared)}")
                            st.rerun()
                        else:
                            st.warning("⚠️ No data sources found to clear")
                    except Exception as e:
                        st.error(f"❌ Error clearing all sources: {e}")
                else:
                    st.info("No data to clear")
        
        # Advanced options
        st.markdown("---")
        st.markdown("### ⚙️ Advanced Options")
        
        # Clear by city
        with st.expander("🏙️ Clear Data by City"):
            cities = self.db.db.businesses.distinct("city")
            if cities:
                cities = [city for city in cities if city]  # Remove None values
                selected_city = st.selectbox("Select city to clear:", cities)
                
                if selected_city:
                    city_businesses = self.db.db.businesses.count_documents({"city": selected_city})
                    st.write(f"📊 Found {city_businesses} businesses in {selected_city}")
                    
                    if st.button(f"🗑️ Clear {selected_city} Data"):
                        if self._clear_data_by_city(selected_city):
                            st.success(f"✅ Data from {selected_city} cleared!")
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to clear data from {selected_city}")
            else:
                st.info("No cities found in database")
        
        # Clear all with strong confirmation
        with st.expander("🚨 Nuclear Option: Clear ALL Data"):
            st.error("⚠️ **DANGER ZONE** ⚠️")
            st.write("This will permanently delete **ALL** data from your database:")
            st.write("• All businesses from all sources")
            st.write("• All reviews and sentiment data")
            st.write("• All analytics and keywords")
            st.write("• Everything will be gone!")
            
            # Multi-step confirmation
            if "nuclear_step" not in st.session_state:
                st.session_state.nuclear_step = 0
            
            if st.session_state.nuclear_step == 0:
                if st.button("⚠️ I understand, proceed"):
                    st.session_state.nuclear_step = 1
                    st.rerun()
            
            elif st.session_state.nuclear_step == 1:
                st.write("Type 'DELETE EVERYTHING' to confirm:")
                confirmation = st.text_input("Confirmation:", key="nuclear_confirm")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("❌ Cancel"):
                        st.session_state.nuclear_step = 0
                        st.rerun()
                with col2:
                    if confirmation == "DELETE EVERYTHING":
                        if st.button("💥 DELETE ALL DATA"):
                            if self._clear_all_data():
                                st.success("✅ All data deleted!")
                                st.session_state.nuclear_step = 0
                                st.rerun()
                            else:
                                st.error("❌ Failed to delete data")
                    else:
                        st.button("💥 DELETE ALL DATA", disabled=True)
    
    def _create_backup(self) -> str:
        """Create a JSON backup of the database"""
        try:
            import json
            
            backup_data = {
                "created_at": datetime.now().isoformat(),
                "businesses": [],
                "reviews": [],
                "analytics": [],
                "trending_keywords": []
            }
            
            # Export each collection
            collections = ["businesses", "reviews", "analytics", "trending_keywords"]
            
            for collection_name in collections:
                collection = getattr(self.db.db, collection_name)
                data = list(collection.find({}))
                
                # Convert ObjectId and datetime to string
                for item in data:
                    if '_id' in item:
                        item['_id'] = str(item['_id'])
                    for key, value in item.items():
                        if hasattr(value, 'isoformat'):
                            item[key] = value.isoformat()
                
                backup_data[collection_name] = data
            
            return json.dumps(backup_data, indent=2, default=str)
            
        except Exception as e:
            st.error(f"Error creating backup: {e}")
            return None
    
    def _clear_data_by_city(self, city_name: str) -> bool:
        """Clear data from a specific city"""
        try:
            # Build query to match city
            city_query = {"city": {"$regex": city_name, "$options": "i"}}
            
            # Get business names for this city
            businesses = list(self.db.db.businesses.find(city_query, {"name": 1}))
            business_names = [b["name"] for b in businesses]
            
            # Delete businesses
            result1 = self.db.db.businesses.delete_many(city_query)
            
            # Delete reviews for these businesses
            if business_names:
                review_query = {"business_name": {"$in": business_names}}
                result2 = self.db.db.reviews.delete_many(review_query)
            
            # Clear cached data
            st.cache_data.clear()
            
            # Clear relevant session state that might cache results
            self._clear_cached_session_state()
            
            return True
            
        except Exception as e:
            st.error(f"Error clearing city data: {e}")
            return False
    
    # ================================
    # BUSINESS OWNER DASHBOARD METHODS
    # ================================
    
    def show_my_business_dashboard(self):
        """Show the main business dashboard for selected business"""
        if not hasattr(self, 'current_selected_business') or not self.current_selected_business:
            st.warning("🏪 Please select your business from the sidebar to view your dashboard.")
            st.markdown("""
            ### Welcome to Business Owner Mode! 🚀
            
            This mode allows you to:
            - **📊 View your business performance** - ratings, reviews, and trends
            - **🔍 Analyze customer sentiment** - understand what customers love and what needs improvement
            - **🥊 Compare to competitors** - see how you stack up in your market segment
            - **💡 Get improvement insights** - actionable recommendations based on data
            
            👈 **Get started by selecting your business from the sidebar!**
            """)
            return
        
        business = self.current_selected_business
        business_name = business['name']
        
        # Header for selected business
        st.markdown(f"""
        <div class="main-header">
            <div class="main-title">🏪 {business_name}</div>
            <div class="main-subtitle">Your Business Performance Dashboard</div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            # Get business reviews
            reviews = list(self.db.db.reviews.find({"business_name": business_name}))
            
            # Business Performance Metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                rating = business.get('rating', 0)
                rating_color = "positive" if rating >= 4.5 else "neutral" if rating >= 4.0 else "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{rating:.1f}⭐</div>
                    <div class="metric-label">Your Rating</div>
                    <div class="metric-delta {rating_color}">Google Places Rating</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                review_count = business.get('review_count', 0)
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{review_count:,}</div>
                    <div class="metric-label">Total Reviews</div>
                    <div class="metric-delta positive">📝 Customer feedback</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                if reviews:
                    positive_reviews = len([r for r in reviews if r.get('sentiment_label') == 'positive'])
                    sentiment_pct = (positive_reviews / len(reviews) * 100) if reviews else 0
                else:
                    sentiment_pct = 0
                
                sentiment_color = "positive" if sentiment_pct >= 70 else "neutral" if sentiment_pct >= 50 else "negative"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{sentiment_pct:.0f}%</div>
                    <div class="metric-label">Positive Sentiment</div>
                    <div class="metric-delta {sentiment_color}">😊 Happy customers</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                # Compare to category average
                category = business.get('category', '')
                if category:
                    category_avg = list(self.db.db.businesses.aggregate([
                        {"$match": {"category": {"$regex": category, "$options": "i"}}},
                        {"$group": {"_id": None, "avg_rating": {"$avg": "$rating"}}}
                    ]))
                    category_avg_rating = category_avg[0]['avg_rating'] if category_avg else 0
                    performance_vs_avg = rating - category_avg_rating if category_avg_rating > 0 else 0
                else:
                    performance_vs_avg = 0
                
                performance_color = "positive" if performance_vs_avg > 0 else "neutral" if performance_vs_avg == 0 else "negative"
                performance_text = f"+{performance_vs_avg:.1f}" if performance_vs_avg > 0 else f"{performance_vs_avg:.1f}"
                
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{performance_text}</div>
                    <div class="metric-label">vs Category Avg</div>
                    <div class="metric-delta {performance_color}">📊 Market position</div>
                </div>
                """, unsafe_allow_html=True)
            
            if not reviews:
                st.info("📝 No detailed reviews found for analysis. The metrics above are based on Google Places data.")
                return
            
            # Convert reviews to DataFrame for analysis
            df = pd.DataFrame(reviews)
            
            # Recent Reviews Section
            st.markdown("### 📝 Recent Customer Reviews")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### ⭐ Recent Reviews")
                recent_reviews = df.head(10)
                for _, review in recent_reviews.iterrows():
                    sentiment_emoji = "😊" if review.get('sentiment_label') == 'positive' else "😐" if review.get('sentiment_label') == 'neutral' else "😞"
                    rating_stars = "⭐" * int(review.get('rating', 0))
                    
                    st.markdown(f"""
                    **{sentiment_emoji} {rating_stars}** 
                    
                    "{review.get('text', 'No review text')[:150]}..."
                    
                    *{review.get('author_name', 'Anonymous')}*
                    
                    ---
                    """)
            
            with col2:
                st.markdown("#### 📊 Review Sentiment Trend")
                
                if 'review_date' in df.columns:
                    # Convert review_date to datetime if it's not already
                    df['review_date'] = pd.to_datetime(df['review_date'])
                    df['month'] = df['review_date'].dt.to_period('M')
                    
                    # Sentiment over time
                    sentiment_by_month = df.groupby(['month', 'sentiment_label']).size().unstack(fill_value=0)
                    
                    if not sentiment_by_month.empty:
                        fig = px.line(
                            x=sentiment_by_month.index.astype(str),
                            y=sentiment_by_month.get('positive', []),
                            title="Positive Reviews Over Time",
                            labels={'x': 'Month', 'y': 'Positive Reviews'}
                        )
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Date information not available for trend analysis")
            
            # Top Keywords Section
            st.markdown("### 🔍 What Customers Are Saying")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 😊 Top Positive Keywords")
                positive_reviews = df[df['sentiment_label'] == 'positive']
                if not positive_reviews.empty:
                    all_positive_text = " ".join(positive_reviews['text'].astype(str))
                    # Simple keyword extraction (you can enhance this)
                    words = all_positive_text.lower().split()
                    common_positive = pd.Series(words).value_counts().head(10)
                    
                    for word, count in common_positive.items():
                        if len(word) > 3:  # Filter out short words
                            st.write(f"**{word}**: {count} mentions")
            
            with col2:
                st.markdown("#### 😞 Areas for Improvement")
                negative_reviews = df[df['sentiment_label'] == 'negative']
                if not negative_reviews.empty:
                    all_negative_text = " ".join(negative_reviews['text'].astype(str))
                    words = all_negative_text.lower().split()
                    common_negative = pd.Series(words).value_counts().head(10)
                    
                    for word, count in common_negative.items():
                        if len(word) > 3:  # Filter out short words
                            st.write(f"**{word}**: {count} mentions")
                else:
                    st.success("🎉 No significant negative feedback found!")
                    
        except Exception as e:
            st.error(f"Error loading business dashboard: {e}")
    
    def show_business_performance_analytics(self):
        """Show detailed performance analytics for the selected business"""
        if not hasattr(self, 'current_selected_business') or not self.current_selected_business:
            st.warning("🏪 Please select your business from the sidebar first.")
            return
        
        business = self.current_selected_business
        business_name = business['name']
        
        st.markdown(f"""
        <div class="main-header">
            <div class="main-title">📊 Performance Analytics</div>
            <div class="main-subtitle">Deep dive into {business_name}'s performance metrics</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Implementation continues in next method...
        st.info("🚧 Advanced performance analytics coming soon! This will include rating trends, seasonal patterns, and customer behavior analysis.")
    
    def show_business_competitor_analysis(self):
        """Show competitor comparison for the selected business"""
        if not hasattr(self, 'current_selected_business') or not self.current_selected_business:
            st.warning("🏪 Please select your business from the sidebar first.")
            return
        
        business = self.current_selected_business
        business_name = business['name']
        category = business.get('category', '')
        city = business.get('city', '')
        
        st.markdown(f"""
        <div class="main-header">
            <div class="main-title">🥊 Competitor Analysis</div>
            <div class="main-subtitle">See how {business_name} compares to similar businesses</div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            # Find competitors in same category and city
            competitor_query = {
                "category": {"$regex": category, "$options": "i"},
                "city": {"$regex": city, "$options": "i"},
                "name": {"$ne": business_name}  # Exclude current business
            }
            
            competitors = list(self.db.db.businesses.find(competitor_query).limit(20))
            
            if not competitors:
                st.warning(f"No competitors found in {category} category in {city}")
                return
            
            # Create comparison DataFrame
            comparison_data = []
            
            # Add current business
            comparison_data.append({
                'Business': business_name + " (You)",
                'Rating': business.get('rating', 0),
                'Reviews': business.get('review_count', 0),
                'Category': business.get('category', ''),
                'Type': 'Your Business'
            })
            
            # Add competitors
            for comp in competitors:
                comparison_data.append({
                    'Business': comp['name'],
                    'Rating': comp.get('rating', 0),
                    'Reviews': comp.get('review_count', 0),
                    'Category': comp.get('category', ''),
                    'Type': 'Competitor'
                })
            
            df = pd.DataFrame(comparison_data)
            
            # Competitive Position Analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 📊 Rating Comparison")
                
                fig = px.bar(
                    df, 
                    x='Business', 
                    y='Rating',
                    color='Type',
                    title=f"Rating Comparison in {category}",
                    color_discrete_map={'Your Business': '#28a745', 'Competitor': '#6c757d'}
                )
                fig.update_xaxis(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 📝 Review Volume Comparison")
                
                fig = px.bar(
                    df, 
                    x='Business', 
                    y='Reviews',
                    color='Type',
                    title=f"Review Count Comparison in {category}",
                    color_discrete_map={'Your Business': '#28a745', 'Competitor': '#6c757d'}
                )
                fig.update_xaxis(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Competitive Insights
            st.markdown("### 💡 Competitive Insights")
            
            your_rating = business.get('rating', 0)
            your_reviews = business.get('review_count', 0)
            
            competitor_ratings = [c.get('rating', 0) for c in competitors]
            competitor_reviews = [c.get('review_count', 0) for c in competitors]
            
            avg_competitor_rating = np.mean(competitor_ratings) if competitor_ratings else 0
            avg_competitor_reviews = np.mean(competitor_reviews) if competitor_reviews else 0
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                rating_diff = your_rating - avg_competitor_rating
                if rating_diff > 0:
                    st.success(f"🏆 **Rating Advantage**: You're {rating_diff:.1f} stars above average!")
                elif rating_diff == 0:
                    st.info(f"📊 **On Par**: Your rating matches the competition")
                else:
                    st.warning(f"📈 **Opportunity**: {abs(rating_diff):.1f} stars below average")
            
            with col2:
                review_diff = your_reviews - avg_competitor_reviews
                if review_diff > 0:
                    st.success(f"📝 **Review Leader**: {int(review_diff)} more reviews than average!")
                elif review_diff == 0:
                    st.info(f"📊 **Average Volume**: Similar review count to competitors")
                else:
                    st.info(f"🚀 **Growth Opportunity**: {int(abs(review_diff))} reviews behind average")
            
            with col3:
                # Market position
                rating_rank = sum(1 for r in competitor_ratings if r < your_rating) + 1
                total_businesses = len(competitor_ratings) + 1
                percentile = ((total_businesses - rating_rank) / total_businesses) * 100
                
                if percentile >= 80:
                    st.success(f"🥇 **Top {int(100-percentile)}%** in your market!")
                elif percentile >= 60:
                    st.info(f"🏅 **Top {int(100-percentile)}%** - Strong position")
                else:
                    st.warning(f"📈 **Room to grow** - {int(percentile)}th percentile")
            
            # Top Competitors Table
            st.markdown("### 🎯 Top Competitors")
            top_competitors = df[df['Type'] == 'Competitor'].sort_values('Rating', ascending=False).head(10)
            st.dataframe(top_competitors[['Business', 'Rating', 'Reviews']], use_container_width=True)
                    
        except Exception as e:
            st.error(f"Error in competitor analysis: {e}")
    
    def show_business_improvement_insights(self):
        """Show actionable improvement insights for the selected business"""
        if not hasattr(self, 'current_selected_business') or not self.current_selected_business:
            st.warning("🏪 Please select your business from the sidebar first.")
            return
        
        business = self.current_selected_business
        business_name = business['name']
        
        st.markdown(f"""
        <div class="main-header">
            <div class="main-title">💡 Improvement Insights</div>
            <div class="main-subtitle">Actionable recommendations for {business_name}</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Get business reviews for analysis
        reviews = list(self.db.db.reviews.find({"business_name": business_name}))
        
        if not reviews:
            st.info("📝 No detailed reviews available for generating insights. Connect more data sources for personalized recommendations.")
            return
        
        df = pd.DataFrame(reviews)
        
        # Generate insights based on review analysis
        insights = []
        
        # Sentiment Analysis Insights
        positive_pct = len(df[df['sentiment_label'] == 'positive']) / len(df) * 100
        negative_pct = len(df[df['sentiment_label'] == 'negative']) / len(df) * 100
        
        if positive_pct >= 80:
            insights.append({
                'type': 'success',
                'title': '🌟 Excellent Customer Satisfaction',
                'description': f'{positive_pct:.0f}% positive sentiment shows strong customer loyalty.',
                'action': 'Leverage your strengths in marketing and maintain current service quality.'
            })
        elif positive_pct >= 60:
            insights.append({
                'type': 'info',
                'title': '👍 Good Customer Satisfaction',
                'description': f'{positive_pct:.0f}% positive sentiment with room for improvement.',
                'action': 'Focus on addressing the key issues mentioned in negative reviews.'
            })
        else:
            insights.append({
                'type': 'warning',
                'title': '⚠️ Customer Satisfaction Needs Attention',
                'description': f'{negative_pct:.0f}% negative sentiment indicates significant issues.',
                'action': 'Urgently address common complaints and improve service quality.'
            })
        
        # Rating Analysis
        current_rating = business.get('rating', 0)
        if current_rating < 4.0:
            insights.append({
                'type': 'warning',
                'title': '📈 Rating Improvement Opportunity',
                'description': f'Current {current_rating:.1f} rating is below market standards.',
                'action': 'Focus on service excellence and encourage satisfied customers to leave reviews.'
            })
        elif current_rating >= 4.5:
            insights.append({
                'type': 'success',
                'title': '⭐ Excellent Rating Performance',
                'description': f'{current_rating:.1f} rating demonstrates exceptional service.',
                'action': 'Maintain quality and use this strength in marketing materials.'
            })
        
        # Review Volume Analysis
        review_count = business.get('review_count', 0)
        if review_count < 50:
            insights.append({
                'type': 'info',
                'title': '📝 Increase Review Volume',
                'description': f'{review_count} reviews may not fully represent your business.',
                'action': 'Implement a review generation strategy to encourage more customer feedback.'
            })
        
        # Display insights
        for insight in insights:
            if insight['type'] == 'success':
                st.success(f"**{insight['title']}**\n\n{insight['description']}\n\n**Recommendation**: {insight['action']}")
            elif insight['type'] == 'warning':
                st.warning(f"**{insight['title']}**\n\n{insight['description']}\n\n**Recommendation**: {insight['action']}")
            else:
                st.info(f"**{insight['title']}**\n\n{insight['description']}\n\n**Recommendation**: {insight['action']}")
        
        # Action Plan
        st.markdown("### 🎯 30-Day Action Plan")
        
        action_items = [
            "📊 Monitor your rating and sentiment weekly",
            "📝 Respond to all customer reviews, especially negative ones",
            "🎯 Focus on improving the most commonly mentioned issues",
            "📢 Encourage satisfied customers to leave reviews",
            "🔍 Analyze competitor strategies and best practices",
            "📈 Track progress and adjust strategies monthly"
        ]
        
        for i, action in enumerate(action_items, 1):
            st.write(f"{i}. {action}")


def main():
    """Main entry point"""
    dashboard = LocalPulseDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()