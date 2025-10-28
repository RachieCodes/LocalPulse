import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set page config
st.set_page_config(
    page_title="LocalPulse Demo Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
</style>
""", unsafe_allow_html=True)

# Generate demo data
@st.cache_data
def generate_demo_data():
    # Demo businesses
    businesses = []
    categories = ['Restaurant', 'Cafe', 'Retail', 'Service', 'Entertainment']
    for i in range(50):
        businesses.append({
            'name': f'Business {i+1}',
            'category': random.choice(categories),
            'rating': round(random.uniform(3.0, 5.0), 1),
            'review_count': random.randint(10, 500),
            'address': f'{random.randint(100, 999)} Main St',
            'latitude': 40.7128 + random.uniform(-0.1, 0.1),
            'longitude': -74.0060 + random.uniform(-0.1, 0.1)
        })
    
    # Demo reviews
    reviews = []
    sentiments = ['positive', 'neutral', 'negative']
    for i in range(200):
        reviews.append({
            'business_id': f'business_{random.randint(1, 50)}',
            'business_name': f'Business {random.randint(1, 50)}',
            'rating': random.randint(1, 5),
            'review_text': f'This is a sample review {i+1}. Lorem ipsum dolor sit amet.',
            'review_date': datetime.now() - timedelta(days=random.randint(1, 365)),
            'sentiment_score': random.uniform(-1, 1),
            'sentiment_label': random.choice(sentiments)
        })
    
    # Demo keywords
    keywords = [
        {'text': 'great', 'weight': 0.95, 'count': 45},
        {'text': 'service', 'weight': 0.85, 'count': 38},
        {'text': 'food', 'weight': 0.80, 'count': 42},
        {'text': 'friendly', 'weight': 0.75, 'count': 32},
        {'text': 'clean', 'weight': 0.70, 'count': 28},
        {'text': 'atmosphere', 'weight': 0.65, 'count': 25},
        {'text': 'delicious', 'weight': 0.60, 'count': 30},
        {'text': 'recommend', 'weight': 0.55, 'count': 22},
        {'text': 'excellent', 'weight': 0.50, 'count': 18},
        {'text': 'amazing', 'weight': 0.45, 'count': 15}
    ]
    
    return businesses, reviews, keywords

def main():
    # Header
    st.markdown('<div class="main-header">📊 LocalPulse Demo Dashboard</div>', unsafe_allow_html=True)
    
    st.info("🚀 This is a demo version with sample data. To use real data, install MongoDB and Redis, then run the full setup.")
    
    # Generate demo data
    businesses, reviews, keywords = generate_demo_data()
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎛️ Filters")
        category_filter = st.selectbox("Business Category", ["All"] + list(set([b['category'] for b in businesses])))
        
        st.markdown("### ℹ️ Demo Info")
        st.write("This demo shows:")
        st.write("• Sample business data")
        st.write("• Generated reviews")
        st.write("• Mock analytics")
        st.write("• All dashboard features")
    
    # Filter data
    if category_filter != "All":
        filtered_businesses = [b for b in businesses if b['category'] == category_filter]
    else:
        filtered_businesses = businesses
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🏆 Top Businesses", "📈 Sentiment Analysis", "☁️ Keywords", "⏰ Time Analytics"])
    
    with tab1:
        st.header("🏆 Top-Rated Businesses")
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Businesses", len(filtered_businesses))
        with col2:
            avg_rating = np.mean([b['rating'] for b in filtered_businesses])
            st.metric("Average Rating", f"{avg_rating:.1f}")
        with col3:
            total_reviews = sum([b['review_count'] for b in filtered_businesses])
            st.metric("Total Reviews", f"{total_reviews:,}")
        with col4:
            categories = len(set([b['category'] for b in filtered_businesses]))
            st.metric("Categories", categories)
        
        # Top businesses table
        df_businesses = pd.DataFrame(filtered_businesses)
        st.subheader("📋 Top Businesses")
        st.dataframe(
            df_businesses[['name', 'category', 'rating', 'review_count', 'address']].head(10),
            use_container_width=True
        )
        
        # Charts
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📊 Rating Distribution")
            rating_counts = df_businesses['rating'].value_counts().sort_index()
            fig = px.bar(x=rating_counts.index, y=rating_counts.values, 
                        labels={'x': 'Rating', 'y': 'Count'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🏪 Category Distribution")
            category_counts = df_businesses['category'].value_counts()
            fig = px.pie(values=category_counts.values, names=category_counts.index)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("📈 Sentiment Analysis")
        
        df_reviews = pd.DataFrame(reviews)
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            avg_sentiment = df_reviews['sentiment_score'].mean()
            st.metric("Average Sentiment", f"{avg_sentiment:.3f}")
        with col2:
            positive_reviews = len(df_reviews[df_reviews['sentiment_score'] > 0.05])
            st.metric("Positive Reviews", positive_reviews)
        with col3:
            negative_reviews = len(df_reviews[df_reviews['sentiment_score'] < -0.05])
            st.metric("Negative Reviews", negative_reviews)
        with col4:
            st.metric("Total Reviews", len(df_reviews))
        
        # Charts
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("😊 Sentiment Distribution")
            sentiment_counts = df_reviews['sentiment_label'].value_counts()
            colors = {'positive': '#00cc66', 'neutral': '#ffcc00', 'negative': '#ff6666'}
            fig = px.pie(values=sentiment_counts.values, names=sentiment_counts.index,
                        color=sentiment_counts.index, color_discrete_map=colors)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📊 Sentiment Score Distribution")
            fig = px.histogram(df_reviews, x='sentiment_score', nbins=20)
            st.plotly_chart(fig, use_container_width=True)
        
        # Sentiment over time
        st.subheader("📈 Sentiment Trend Over Time")
        df_reviews['month'] = pd.to_datetime(df_reviews['review_date']).dt.to_period('M')
        monthly_sentiment = df_reviews.groupby('month').agg({
            'sentiment_score': 'mean',
            'rating': 'mean'
        }).reset_index()
        monthly_sentiment['month_str'] = monthly_sentiment['month'].astype(str)
        
        fig = px.line(monthly_sentiment, x='month_str', y='sentiment_score', 
                     title='Monthly Average Sentiment')
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.header("☁️ Keyword Insights")
        
        # Keyword frequency chart
        st.subheader("📈 Top Keywords")
        df_keywords = pd.DataFrame(keywords)
        fig = px.bar(df_keywords.head(10), x='count', y='text', orientation='h',
                    title="Most Frequent Keywords")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Keyword table
        st.subheader("📋 Keyword Details")
        st.dataframe(df_keywords, use_container_width=True)
        
        # Word cloud simulation (text display)
        st.subheader("☁️ Word Cloud Preview")
        st.info("In the full version, this would show an interactive word cloud visualization")
        
        # Display keywords as tags
        keyword_text = " | ".join([f"**{kw['text']}** ({kw['count']})" for kw in keywords[:15]])
        st.markdown(keyword_text)
    
    with tab4:
        st.header("⏰ Time Analytics")
        
        # Review volume over time
        st.subheader("📊 Review Volume Over Time")
        df_reviews['month'] = pd.to_datetime(df_reviews['review_date']).dt.to_period('M')
        monthly_reviews = df_reviews.groupby('month').size().reset_index(name='review_count')
        monthly_reviews['month_str'] = monthly_reviews['month'].astype(str)
        
        fig = px.line(monthly_reviews, x='month_str', y='review_count',
                     title="Monthly Review Volume")
        st.plotly_chart(fig, use_container_width=True)
        
        # Day of week analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📅 Reviews by Day of Week")
            df_reviews['day_of_week'] = pd.to_datetime(df_reviews['review_date']).dt.day_name()
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_counts = df_reviews['day_of_week'].value_counts().reindex(day_order)
            
            fig = px.bar(x=day_counts.index, y=day_counts.values,
                        title="Review Distribution by Day")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🕐 Peak Hours Analysis")
            # Generate random hourly data
            hours = list(range(24))
            review_counts = [random.randint(5, 50) for _ in hours]
            
            fig = px.bar(x=hours, y=review_counts,
                        title="Review Volume by Hour of Day")
            st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("**LocalPulse Demo** - To access full features with real data, complete the setup with MongoDB and Redis")

if __name__ == "__main__":
    main()