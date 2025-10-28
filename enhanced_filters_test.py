#!/usr/bin/env python3
"""
Test the enhanced filtering system without NLTK dependencies
"""

import sys
import os
sys.path.append('.')

import streamlit as st
from datetime import datetime, timedelta
from utils.new_places_api import NewPlacesAPISearch
from database.mongo_client import MongoDatabase

st.set_page_config(page_title="LocalPulse - Enhanced Filters Test", page_icon="🔧")

st.title("🔧 LocalPulse - Enhanced Filtering System Test")

# Initialize components
@st.cache_resource
def get_components():
    searcher = NewPlacesAPISearch()
    db = MongoDatabase()
    return searcher, db

searcher, db = get_components()

# Enhanced sidebar filters
st.sidebar.header("🎛️ Enhanced Filters")

# 1. Primary city selection with free-form input
st.sidebar.subheader("🌍 City Search")
st.sidebar.write("Search for businesses in any city in the United States!")

# City input method selector
input_method = st.sidebar.radio(
    "How would you like to select a city?",
    options=["🔍 Search any city", "📋 Choose from popular cities"],
    index=0
)

if input_method == "🔍 Search any city":
    primary_city = st.sidebar.text_input(
        "Enter any city name",
        placeholder="e.g., Austin TX, Portland OR, Nashville TN, Miami FL",
        help="Type any city name - get real business data from Google Places API!"
    )
    if not primary_city:
        primary_city = "All Cities"
else:
    primary_city = st.sidebar.selectbox(
        "Choose from popular cities",
        options=["All Cities", "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin", "Seattle", 
                "Miami", "Atlanta", "Boston", "Denver", "Portland", "Las Vegas", "Nashville"],
        index=0
    )

# 2. Enhanced time period selection (answers questions #2 & #3)
st.sidebar.subheader("📅 Time Period Analysis")

time_period = st.sidebar.selectbox(
    "Choose time period",
    options=["Last 30 days", "Last 90 days", "Last 6 months", "Last year", "Custom range"],
    index=1,
    help="Select predefined period or choose custom range"
)

current_date = datetime.now().date()

if time_period == "Last 30 days":
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
    date_range = st.sidebar.date_input(
        "Select custom date range",
        value=[current_date - timedelta(days=90), current_date],  # Default end date is always current
        max_value=current_date,
        help="End date defaults to today (answers question #3)"
    )

# 3. Data limit controls (answers question #4)
st.sidebar.subheader("⚡ Performance Controls")

data_limit = st.sidebar.selectbox(
    "Maximum records to analyze",
    options=[100, 500, 1000, 2500, 5000, "All data"],
    index=2,
    help="Limit data for faster performance or choose 'All data' for complete analysis"
)

# Main content
st.header("✅ Enhanced Filtering System")

col1, col2 = st.columns(2)

with col1:
    st.subheader("🎯 Your Current Filters")
    
    # Show active filters
    filters_applied = []
    
    # Time period info
    if len(date_range) == 2:
        days_span = (date_range[1] - date_range[0]).days
        st.success(f"📅 **Time Period**: {time_period}")
        st.caption(f"Analyzing {days_span} days ({date_range[0]} to {date_range[1]})")
        if time_period == "Custom range":
            st.caption("✅ End date defaults to current date!")
    
    # Data limit info
    if data_limit != "All data":
        st.success(f"⚡ **Data Limit**: {data_limit} records")
        st.caption("Limiting data for faster performance")
    else:
        st.warning("📊 **All Data**: May be slower but more complete")

with col2:
    st.subheader("💡 Simplified Search")
    
    st.markdown("""
    **✅ Simplified City Search:**
    - Removed additional location filter for simplicity
    - Primary city search is now the main way to explore
    - Users can search ANY city worldwide
    - Two input methods: free-form search or popular cities
    
    **✅ Enhanced User Experience:**
    - No more confusing multiple location filters
    - Direct city input with real-time Google Places data
    - Clear indication of what city is being analyzed
    - Streamlined interface focused on city exploration
    
    **✅ Global City Support:**
    - Search any city: "Tokyo", "London", "Sydney"
    - US cities: "Austin TX", "Portland OR", "Nashville TN"
    - International: "Toronto Canada", "Vancouver BC"
    - Real business data from Google Places API
    """)

# Test the simplified search functionality
st.header("🔍 Test Simplified City Search")

if primary_city and primary_city != "All Cities":
    if st.button(f"🔍 Search businesses in {primary_city}"):
        with st.spinner(f"Searching for businesses in {primary_city}..."):
            businesses = searcher.search_businesses_near_city(primary_city)
            
            if businesses:
                data_type = "REAL" if searcher.gmaps else "demo"
                st.success(f"Found {len(businesses)} businesses in {primary_city}! ({data_type} data)")
                
                # Show limited results based on data_limit
                limit = min(len(businesses), data_limit if data_limit != "All data" else len(businesses))
                
                st.info(f"Showing {limit} of {len(businesses)} businesses (based on your data limit setting)")
                
                # Show businesses in a more organized way
                for i, biz in enumerate(businesses[:limit]):
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.write(f"**{i+1}. {biz.name}**")
                        st.caption(f"📍 {biz.address}")
                    with col2:
                        st.metric("Rating", f"{biz.rating}⭐")
                    with col3:
                        st.write(f"**{biz.category}**")
                    st.divider()
            else:
                st.error(f"No businesses found for {primary_city}")
                st.info("💡 Try different city formats: 'Austin TX', 'Portland Oregon', 'Miami Florida'")
else:
    st.info("👆 Select or enter a city name above to search for businesses!")

# Show example searches
st.header("💡 Example City Searches")

st.subheader("🇺🇸 US Cities")
us_cities = ["Austin, TX", "Portland, OR", "Nashville, TN", "Miami, FL"]
cols = st.columns(4)
for i, city in enumerate(us_cities):
    with cols[i]:
        if st.button(f"🔍 {city}", key=f"us_{i}"):
            businesses = searcher.search_businesses_near_city(city)
            if businesses:
                st.success(f"Found {len(businesses)} businesses!")
                with st.expander(f"Top business in {city}"):
                    biz = businesses[0]
                    st.write(f"**{biz.name}** ({biz.rating}⭐)")
                    st.write(f"Category: {biz.category}")
                    st.write(f"Address: {biz.address}")

st.subheader("🌍 International Cities")
intl_cities = ["Tokyo, Japan", "London, UK", "Paris, France", "Toronto, Canada"]
cols = st.columns(4)
for i, city in enumerate(intl_cities):
    with cols[i]:
        if st.button(f"🔍 {city}", key=f"intl_{i}"):
            businesses = searcher.search_businesses_near_city(city)
            if businesses:
                st.success(f"Found {len(businesses)} businesses!")
                with st.expander(f"Top business in {city}"):
                    biz = businesses[0]
                    st.write(f"**{biz.name}** ({biz.rating}⭐)")
                    st.write(f"Category: {biz.category}")
                    st.write(f"Address: {biz.address}")
                    st.write(f"Phone: {biz.phone}")

st.subheader("🌏 More International Examples")
more_cities = ["Sydney, Australia", "Berlin, Germany", "Singapore", "Dubai, UAE"]
cols = st.columns(4)
for i, city in enumerate(more_cities):
    with cols[i]:
        if st.button(f"🔍 {city}", key=f"more_{i}"):
            businesses = searcher.search_businesses_near_city(city)
            if businesses:
                st.success(f"Found {len(businesses)} businesses!")
                with st.expander(f"Top business in {city}"):
                    biz = businesses[0]
                    st.write(f"**{biz.name}** ({biz.rating}⭐)")
                    st.write(f"Category: {biz.category}")
                    st.write(f"Address: {biz.address}")

# Show Google API status
st.header("🔧 Google Places API Status")
col1, col2, col3 = st.columns(3)
with col1:
    if searcher.api_key:
        st.success("✅ API Key")
    else:
        st.error("❌ API Key")

with col2:
    if searcher.geocoding_available:
        st.success("✅ Geocoding API")
    else:
        st.error("❌ Geocoding API")

with col3:
    if searcher.places_available:
        st.success("✅ Places API (NEW)")
    else:
        st.error("❌ Places API (NEW)")

if searcher.gmaps:
    st.success("🌐 **REAL DATA MODE** - Your Google Places API is working perfectly!")
else:
    st.warning("🔧 **DEMO MODE** - Enable APIs in Google Cloud Console for real data")