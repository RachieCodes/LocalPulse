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
    
    def run(self):
        """Main dashboard runner"""
        st.set_page_config(
            page_title="LocalPulse Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Initialize filter variables
        self.current_category_filter = ""
        self.current_primary_city = ""
        self.current_data_limit = 1000
        self.current_date_range = [datetime.now().date() - timedelta(days=90), datetime.now().date()]
        
        # Initialize session state and URL params for dropdown persistence across refreshes
        self._initialize_persistent_state()
        
        # Custom CSS
        st.markdown("""
        <style>
        .main-header {
            font-size: 3rem;
            font-weight: bold;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            border-left: 5px solid #1f77b4;
        }
        .sidebar-header {
            font-size: 1.5rem;
            font-weight: bold;
            color: #1f77b4;
            margin-bottom: 1rem;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Header
        st.markdown('<div class="main-header">📊 LocalPulse Dashboard</div>', unsafe_allow_html=True)
        
        # Quick info about database management
        if "show_db_info" not in st.session_state:
            st.session_state.show_db_info = True
        
        if st.session_state.show_db_info:
            with st.container():
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.info("🗃️ **New!** Database management is now available in the sidebar and the 'Database Manager' tab. You can view stats, create backups, and safely remove data entries.")
                with col2:
                    if st.button("✕", help="Hide this message"):
                        st.session_state.show_db_info = False
                        st.rerun()
        
        # Sidebar
        self.create_sidebar()
        
        # Create tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🏆 Top Businesses", 
            "📈 Sentiment Analysis", 
            "☁️ Keyword Insights", 
            "⏰ Time Analytics", 
            "🔍 Competitor Analysis",
            "🗃️ Database Manager"
        ])
        
        # Get current filters (set by sidebar)
        
        with tab1:
            self.show_top_businesses(self.current_category_filter, self.current_primary_city)
        
        with tab2:
            self.show_sentiment_analysis(self.current_category_filter, self.current_primary_city, self.current_date_range)
        
        with tab3:
            self.show_keyword_analysis(self.current_category_filter, self.current_primary_city, self.current_date_range)
        
        with tab4:
            self.show_time_analytics(self.current_category_filter, self.current_primary_city, self.current_date_range)
        
        with tab5:
            self.show_competitor_analysis(self.current_primary_city)
        
        with tab6:
            self.show_database_manager()
    
    def create_sidebar(self):
        """Create sidebar with filters"""
        with st.sidebar:
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
            
            # Enhanced date range filter with predefined periods
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
            
            if st.button("📊 Update Analytics"):
                with st.spinner("Updating analytics..."):
                    try:
                        result = self.pipeline.update_business_analytics()
                        st.success(f"Updated analytics for {result} businesses!")
                    except Exception as e:
                        st.error(f"Error updating analytics: {e}")
            
            # Database management section
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
    
    def show_sentiment_analysis(self, category_filter: str, primary_city: str, date_range: List):
        """Show sentiment analysis dashboard with enhanced filtering"""
        st.header("📈 Sentiment Analysis")
        
        # Show active filters
        self._show_active_filters(category_filter, primary_city)
        
        try:
            # Get sentiment data
            reviews = self.get_reviews_data(category_filter, primary_city, date_range)
            
            if not reviews:
                st.warning("No reviews found with the current filters.")
                return
            
            df = pd.DataFrame(reviews)
            
            # Sentiment metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                avg_sentiment = df['sentiment_score'].mean() if 'sentiment_score' in df.columns else 0
                st.metric("Average Sentiment", f"{avg_sentiment:.3f}")
            
            with col2:
                positive_reviews = len(df[df['sentiment_score'] > 0.05]) if 'sentiment_score' in df.columns else 0
                st.metric("Positive Reviews", positive_reviews)
            
            with col3:
                negative_reviews = len(df[df['sentiment_score'] < -0.05]) if 'sentiment_score' in df.columns else 0
                st.metric("Negative Reviews", negative_reviews)
            
            with col4:
                total_reviews = len(df)
                st.metric("Total Reviews", total_reviews)
            
            # Sentiment distribution
            col1, col2 = st.columns(2)
            
            with col1:
                if 'sentiment_label' in df.columns:
                    st.subheader("😊 Sentiment Distribution")
                    sentiment_counts = df['sentiment_label'].value_counts()
                    fig_sentiment = px.pie(
                        values=sentiment_counts.values,
                        names=sentiment_counts.index,
                        title="Review Sentiment Distribution",
                        color_discrete_map={
                            'positive': '#00cc66',
                            'neutral': '#ffcc00',
                            'negative': '#ff6666'
                        }
                    )
                    st.plotly_chart(fig_sentiment, use_container_width=True)
            
            with col2:
                if 'sentiment_score' in df.columns:
                    st.subheader("📊 Sentiment Score Distribution")
                    fig_hist = px.histogram(
                        df, x='sentiment_score',
                        nbins=30,
                        title="Distribution of Sentiment Scores"
                    )
                    st.plotly_chart(fig_hist, use_container_width=True)
            
            # Sentiment over time
            if 'review_date' in df.columns and 'sentiment_score' in df.columns:
                st.subheader("📈 Sentiment Trend Over Time")
                
                # Prepare data for time series
                df['review_date'] = pd.to_datetime(df['review_date'])
                df['month'] = df['review_date'].dt.to_period('M')
                
                monthly_sentiment = df.groupby('month').agg({
                    'sentiment_score': 'mean',
                    'rating': 'mean'
                }).reset_index()
                
                monthly_sentiment['month_str'] = monthly_sentiment['month'].astype(str)
                
                fig_trend = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig_trend.add_trace(
                    go.Scatter(
                        x=monthly_sentiment['month_str'],
                        y=monthly_sentiment['sentiment_score'],
                        name="Sentiment Score",
                        line=dict(color='blue')
                    ),
                    secondary_y=False,
                )
                
                fig_trend.add_trace(
                    go.Scatter(
                        x=monthly_sentiment['month_str'],
                        y=monthly_sentiment['rating'],
                        name="Average Rating",
                        line=dict(color='red')
                    ),
                    secondary_y=True,
                )
                
                fig_trend.update_xaxes(title_text="Month")
                fig_trend.update_yaxes(title_text="Sentiment Score", secondary_y=False)
                fig_trend.update_yaxes(title_text="Average Rating", secondary_y=True)
                fig_trend.update_layout(title_text="Sentiment and Rating Trends")
                
                st.plotly_chart(fig_trend, use_container_width=True)
            
            # Top positive and negative reviews
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("😍 Most Positive Reviews")
                if 'sentiment_score' in df.columns:
                    top_positive = df.nlargest(5, 'sentiment_score')
                    for _, review in top_positive.iterrows():
                        with st.expander(f"⭐ {review.get('rating', 'N/A')} - {review.get('business_name', 'Unknown')}"):
                            st.write(f"**Sentiment Score:** {review.get('sentiment_score', 0):.3f}")
                            st.write(f"**Review:** {review.get('review_text', 'No text available')[:300]}...")
            
            with col2:
                st.subheader("😞 Most Negative Reviews")
                if 'sentiment_score' in df.columns:
                    top_negative = df.nsmallest(5, 'sentiment_score')
                    for _, review in top_negative.iterrows():
                        with st.expander(f"⭐ {review.get('rating', 'N/A')} - {review.get('business_name', 'Unknown')}"):
                            st.write(f"**Sentiment Score:** {review.get('sentiment_score', 0):.3f}")
                            st.write(f"**Review:** {review.get('review_text', 'No text available')[:300]}...")
                            
        except Exception as e:
            st.error(f"Error loading sentiment analysis: {e}")
    
    def show_keyword_analysis(self, category_filter: str, primary_city: str, date_range: List):
        """Show keyword analysis and word cloud"""
        st.header("☁️ Keyword Insights")
        
        try:
            # Get reviews for keyword analysis
            reviews = self.get_reviews_data(category_filter, primary_city, date_range)
            
            if not reviews:
                st.warning("No reviews found for keyword analysis.")
                return
            
            # Generate trending keywords
            with st.spinner("Generating keyword insights..."):
                keyword_data = self.pipeline.review_processor.get_keyword_cloud_data(reviews, max_keywords=100)
            
            if not keyword_data:
                st.warning("No keywords could be extracted from the reviews.")
                return
            
            # Display word cloud
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("☁️ Word Cloud")
                self.create_word_cloud(keyword_data)
            
            with col2:
                st.subheader("📊 Top Keywords")
                # Show top keywords table
                keywords_df = pd.DataFrame(keyword_data[:20])
                st.dataframe(
                    keywords_df[['text', 'weight', 'count']],
                    column_config={
                        "text": "Keyword",
                        "weight": st.column_config.NumberColumn("TF-IDF Score", format="%.4f"),
                        "count": "Frequency"
                    },
                    use_container_width=True
                )
            
            # Keyword frequency chart
            st.subheader("📈 Keyword Frequency Analysis")
            top_keywords = keyword_data[:20]
            
            fig_keywords = px.bar(
                x=[kw['count'] for kw in top_keywords],
                y=[kw['text'] for kw in top_keywords],
                orientation='h',
                title="Top 20 Keywords by Frequency",
                labels={'x': 'Frequency', 'y': 'Keywords'}
            )
            fig_keywords.update_layout(height=600)
            st.plotly_chart(fig_keywords, use_container_width=True)
            
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
        """Show time-based analytics"""
        st.header("⏰ Time Analytics")
        
        try:
            # Get reviews data
            reviews = self.get_reviews_data(category_filter, primary_city, date_range)
            businesses = self.get_top_businesses_data(category_filter, primary_city)
            
            if not reviews:
                st.warning("No reviews found for time analysis.")
                return
            
            df = pd.DataFrame(reviews)
            df['review_date'] = pd.to_datetime(df['review_date'])
            
            # Review volume over time
            st.subheader("📊 Review Volume Over Time")
            
            df['month'] = df['review_date'].dt.to_period('M')
            monthly_reviews = df.groupby('month').size().reset_index(name='review_count')
            monthly_reviews['month_str'] = monthly_reviews['month'].astype(str)
            
            fig_volume = px.line(
                monthly_reviews,
                x='month_str',
                y='review_count',
                title="Monthly Review Volume",
                labels={'month_str': 'Month', 'review_count': 'Number of Reviews'}
            )
            st.plotly_chart(fig_volume, use_container_width=True)
            
            # Day of week analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📅 Reviews by Day of Week")
                df['day_of_week'] = df['review_date'].dt.day_name()
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                day_counts = df['day_of_week'].value_counts().reindex(day_order)
                
                fig_day = px.bar(
                    x=day_counts.index,
                    y=day_counts.values,
                    title="Review Distribution by Day of Week",
                    labels={'x': 'Day of Week', 'y': 'Number of Reviews'}
                )
                st.plotly_chart(fig_day, use_container_width=True)
            
            with col2:
                st.subheader("🕐 Business Hours Analysis")
                # Show business hours if available
                if businesses:
                    hours_data = []
                    for business in businesses:
                        hours = business.get('hours', {})
                        if isinstance(hours, dict):
                            for day, time_range in hours.items():
                                if time_range and time_range != 'Closed':
                                    hours_data.append({'business': business.get('name'), 'day': day, 'hours': time_range})
                    
                    if hours_data:
                        st.write("Sample business hours:")
                        hours_df = pd.DataFrame(hours_data[:10])
                        st.dataframe(hours_df, use_container_width=True)
                    else:
                        st.info("No business hours data available")
            
            # Peak review times vs business hours
            st.subheader("📈 Peak Review Times Analysis")
            
            # Extract hour from review timestamp
            df['hour'] = df['review_date'].dt.hour
            hourly_reviews = df.groupby('hour').size().reset_index(name='review_count')
            
            fig_hourly = px.bar(
                hourly_reviews,
                x='hour',
                y='review_count',
                title="Review Volume by Hour of Day",
                labels={'hour': 'Hour of Day', 'review_count': 'Number of Reviews'}
            )
            st.plotly_chart(fig_hourly, use_container_width=True)
            
            # Seasonal trends
            st.subheader("🌸 Seasonal Trends")
            df['quarter'] = df['review_date'].dt.quarter
            quarterly_stats = df.groupby('quarter').agg({
                'review_date': 'count',
                'rating': 'mean',
                'sentiment_score': 'mean'
            }).round(3)
            quarterly_stats.columns = ['Review Count', 'Avg Rating', 'Avg Sentiment']
            
            st.dataframe(quarterly_stats, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error in time analytics: {e}")
    
    def show_competitor_analysis(self, primary_city: str):
        """Show competitor comparison"""
        st.header("🔍 Competitor Analysis")
        
        try:
            # Business selection for comparison
            businesses = self.get_top_businesses_data("", primary_city)
            
            if not businesses:
                st.warning("No businesses available for comparison.")
                return
            
            business_options = {b['name']: b['source_id'] for b in businesses[:50]}
            
            selected_businesses = st.multiselect(
                "Select businesses to compare:",
                options=list(business_options.keys()),
                default=list(business_options.keys())[:3] if len(business_options) >= 3 else list(business_options.keys())
            )
            
            if len(selected_businesses) < 2:
                st.warning("Please select at least 2 businesses for comparison.")
                return
            
            # Get competitor data
            business_ids = [business_options[name] for name in selected_businesses]
            competitor_data = self.pipeline.calculate_competitor_metrics(business_ids)
            
            if not competitor_data.get('competitors'):
                st.warning("No data available for selected businesses.")
                return
            
            # Display comparison table
            st.subheader("📊 Business Comparison")
            
            comp_df = pd.DataFrame(competitor_data['competitors'])
            
            # Create comparison chart
            fig_comp = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Rating Comparison', 'Review Count', 'Sentiment Score', 'Category Distribution'),
                specs=[[{"type": "bar"}, {"type": "bar"}],
                       [{"type": "bar"}, {"type": "pie"}]]
            )
            
            # Rating comparison
            fig_comp.add_trace(
                go.Bar(x=comp_df['name'], y=comp_df['rating'], name='Rating'),
                row=1, col=1
            )
            
            # Review count comparison
            fig_comp.add_trace(
                go.Bar(x=comp_df['name'], y=comp_df['review_count'], name='Reviews'),
                row=1, col=2
            )
            
            # Sentiment comparison
            fig_comp.add_trace(
                go.Bar(x=comp_df['name'], y=comp_df['avg_sentiment'], name='Sentiment'),
                row=2, col=1
            )
            
            # Category distribution
            category_counts = comp_df['category'].value_counts()
            fig_comp.add_trace(
                go.Pie(labels=category_counts.index, values=category_counts.values, name='Categories'),
                row=2, col=2
            )
            
            fig_comp.update_layout(height=800, showlegend=False)
            st.plotly_chart(fig_comp, use_container_width=True)
            
            # Market averages
            st.subheader("📈 Market Averages")
            market_avg = competitor_data.get('market_averages', {})
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Average Rating", f"{market_avg.get('rating', 0):.2f}")
            with col2:
                st.metric("Average Reviews", f"{market_avg.get('review_count', 0):,}")
            with col3:
                st.metric("Average Sentiment", f"{market_avg.get('sentiment', 0):.3f}")
            
            # Detailed comparison table
            st.subheader("📋 Detailed Comparison")
            display_df = comp_df[['name', 'category', 'rating', 'review_count', 'avg_sentiment', 'price_range']]
            
            st.dataframe(
                display_df,
                column_config={
                    "name": "Business Name",
                    "category": "Category",
                    "rating": st.column_config.NumberColumn("Rating", format="%.1f ⭐"),
                    "review_count": st.column_config.NumberColumn("Reviews", format="%d"),
                    "avg_sentiment": st.column_config.NumberColumn("Sentiment", format="%.3f"),
                    "price_range": "Price Range"
                },
                use_container_width=True
            )
            
        except Exception as e:
            st.error(f"Error in competitor analysis: {e}")
    
    def get_top_businesses_data(self, category_filter: str, primary_city: str):
        """Get top businesses data with enhanced filtering"""
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
            if date_range and len(date_range) == 2:
                from datetime import datetime, date, time
                start_date = date_range[0]
                end_date = date_range[1]
                
                # Convert to datetime if they are date objects
                if isinstance(start_date, date) and not isinstance(start_date, datetime):
                    start_date = datetime.combine(start_date, time.min)
                if isinstance(end_date, date) and not isinstance(end_date, datetime):
                    end_date = datetime.combine(end_date, time.max)
                
                date_range = [start_date, end_date]
                query["date"] = {"$gte": start_date, "$lte": end_date}
            # If date_range is None, don't add any date filtering to query
            
            # Determine data limit
            limit = self.current_data_limit if hasattr(self, 'current_data_limit') and self.current_data_limit else None
            
            if category_filter:
                # Get businesses in category first
                businesses = self.db.get_businesses_by_category(category_filter, limit=limit or 1000)
                if primary_city:
                    # Filter businesses by city as well
                    businesses = [b for b in businesses if primary_city.lower() in b.get('address', '').lower()]
                
                business_ids = [b.get('source_id') for b in businesses]
                
                # Get reviews for these businesses
                reviews = []
                review_limit_per_business = 20 if limit else 100  # More reviews per business if no limits
                business_limit = min(len(business_ids), 50 if limit else 200)  # More businesses if no limits
                
                for business_id in business_ids[:business_limit]:
                    business_reviews = self.db.get_reviews_for_business(business_id, limit=review_limit_per_business)
                    reviews.extend(business_reviews)
            else:
                # Get reviews by date range and city
                if date_range and len(date_range) == 2:
                    reviews = self.db.get_reviews_by_date_range(date_range[0], date_range[1])
                else:
                    # No date filtering - get all reviews matching other criteria
                    query_limit = limit or 10000  # Much higher limit when no performance restrictions
                    reviews = list(self.db.db.reviews.find(query).limit(query_limit))
                
                # Filter by city if specified
                if primary_city:
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


def main():
    """Main entry point"""
    dashboard = LocalPulseDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()