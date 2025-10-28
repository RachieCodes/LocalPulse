import nltk
import re
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import Counter
import pandas as pd
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import logging

# Download required NLTK data
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('vader_lexicon', quiet=True)
except:
    pass

from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer


class SentimentAnalyzer:
    """Sentiment analysis for reviews"""
    
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        self.stop_words = set(stopwords.words('english'))
        
    def analyze_sentiment(self, text: str) -> Tuple[float, str]:
        """
        Analyze sentiment of text
        Returns: (sentiment_score, sentiment_label)
        """
        if not text:
            return 0.0, 'neutral'
        
        # Use VADER for sentiment analysis
        scores = self.sia.polarity_scores(text)
        compound_score = scores['compound']
        
        # Determine sentiment label
        if compound_score >= 0.05:
            label = 'positive'
        elif compound_score <= -0.05:
            label = 'negative'
        else:
            label = 'neutral'
            
        return compound_score, label
    
    def analyze_sentiment_textblob(self, text: str) -> Tuple[float, str]:
        """Alternative sentiment analysis using TextBlob"""
        if not text:
            return 0.0, 'neutral'
            
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        
        if polarity > 0.1:
            label = 'positive'
        elif polarity < -0.1:
            label = 'negative'
        else:
            label = 'neutral'
            
        return polarity, label


class KeywordExtractor:
    """Extract keywords and phrases from reviews"""
    
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.custom_stop_words = {
            'place', 'restaurant', 'food', 'service', 'time', 'really',
            'good', 'great', 'nice', 'love', 'like', 'went', 'got',
            'ordered', 'came', 'back', 'would', 'definitely', 'highly'
        }
        self.all_stop_words = self.stop_words.union(self.custom_stop_words)
        
    def clean_text(self, text: str) -> str:
        """Clean and preprocess text"""
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters but keep spaces
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text
    
    def extract_keywords(self, text: str, max_keywords: int = 10) -> List[str]:
        """Extract keywords from single text"""
        cleaned_text = self.clean_text(text)
        words = cleaned_text.split()
        
        # Filter out stop words and short words
        keywords = [word for word in words 
                   if word not in self.all_stop_words and len(word) > 2]
        
        # Count frequency and return top keywords
        word_freq = Counter(keywords)
        return [word for word, count in word_freq.most_common(max_keywords)]
    
    def extract_keywords_tfidf(self, texts: List[str], max_keywords: int = 50) -> List[Tuple[str, float]]:
        """Extract keywords using TF-IDF from multiple texts"""
        if not texts:
            return []
        
        # Clean texts
        cleaned_texts = [self.clean_text(text) for text in texts if text]
        
        if not cleaned_texts:
            return []
        
        # TF-IDF vectorization
        vectorizer = TfidfVectorizer(
            max_features=500,
            stop_words=list(self.all_stop_words),
            ngram_range=(1, 2),  # Include bigrams
            min_df=2,  # Word must appear in at least 2 documents
            max_df=0.8  # Word shouldn't appear in more than 80% of documents
        )
        
        try:
            tfidf_matrix = vectorizer.fit_transform(cleaned_texts)
            feature_names = vectorizer.get_feature_names_out()
            
            # Get average TF-IDF scores
            mean_scores = tfidf_matrix.mean(axis=0).A1
            
            # Create keyword-score pairs and sort
            keyword_scores = list(zip(feature_names, mean_scores))
            keyword_scores.sort(key=lambda x: x[1], reverse=True)
            
            return keyword_scores[:max_keywords]
        
        except Exception as e:
            logging.error(f"Error in TF-IDF extraction: {e}")
            return []
    
    def extract_phrases(self, text: str, max_phrases: int = 5) -> List[str]:
        """Extract meaningful phrases from text"""
        cleaned_text = self.clean_text(text)
        
        # Simple phrase extraction - look for adjective-noun combinations
        words = cleaned_text.split()
        phrases = []
        
        for i in range(len(words) - 1):
            phrase = f"{words[i]} {words[i+1]}"
            if (len(phrase) > 5 and 
                words[i] not in self.all_stop_words and 
                words[i+1] not in self.all_stop_words):
                phrases.append(phrase)
        
        # Count frequency and return top phrases
        phrase_freq = Counter(phrases)
        return [phrase for phrase, count in phrase_freq.most_common(max_phrases)]


class ReviewProcessor:
    """Process and analyze review data"""
    
    def __init__(self):
        self.sentiment_analyzer = SentimentAnalyzer()
        self.keyword_extractor = KeywordExtractor()
    
    def process_review(self, review_text: str) -> Dict:
        """Process a single review"""
        if not review_text:
            return {}
        
        # Sentiment analysis
        sentiment_score, sentiment_label = self.sentiment_analyzer.analyze_sentiment(review_text)
        
        # Extract keywords
        keywords = self.keyword_extractor.extract_keywords(review_text)
        
        # Extract phrases
        phrases = self.keyword_extractor.extract_phrases(review_text)
        
        return {
            'sentiment_score': sentiment_score,
            'sentiment_label': sentiment_label,
            'keywords': keywords,
            'phrases': phrases,
            'word_count': len(review_text.split()),
            'character_count': len(review_text)
        }
    
    def process_reviews_batch(self, reviews: List[Dict]) -> List[Dict]:
        """Process multiple reviews"""
        processed_reviews = []
        
        for review in reviews:
            review_text = review.get('review_text', '')
            processed_data = self.process_review(review_text)
            
            # Update review with processed data
            review.update(processed_data)
            processed_reviews.append(review)
        
        return processed_reviews
    
    def get_sentiment_trend(self, reviews: List[Dict], period: str = 'month') -> List[Dict]:
        """Calculate sentiment trend over time"""
        if not reviews:
            return []
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(reviews)
        
        if 'review_date' not in df.columns or 'sentiment_score' not in df.columns:
            return []
        
        # Ensure review_date is datetime
        df['review_date'] = pd.to_datetime(df['review_date'])
        
        # Group by period
        if period == 'month':
            df['period'] = df['review_date'].dt.to_period('M')
        elif period == 'week':
            df['period'] = df['review_date'].dt.to_period('W')
        else:  # day
            df['period'] = df['review_date'].dt.to_period('D')
        
        # Calculate average sentiment per period
        trend_data = df.groupby('period').agg({
            'sentiment_score': ['mean', 'count'],
            'rating': 'mean'
        }).round(3)
        
        # Flatten column names
        trend_data.columns = ['avg_sentiment', 'review_count', 'avg_rating']
        trend_data = trend_data.reset_index()
        
        # Convert to list of dictionaries
        trend_list = []
        for _, row in trend_data.iterrows():
            trend_list.append({
                'period': str(row['period']),
                'avg_sentiment': row['avg_sentiment'],
                'review_count': row['review_count'],
                'avg_rating': row['avg_rating']
            })
        
        return trend_list
    
    def get_keyword_cloud_data(self, reviews: List[Dict], max_keywords: int = 100) -> List[Dict]:
        """Get keyword data for word cloud"""
        review_texts = [review.get('review_text', '') for review in reviews if review.get('review_text')]
        
        if not review_texts:
            return []
        
        # Extract keywords using TF-IDF
        keywords = self.keyword_extractor.extract_keywords_tfidf(review_texts, max_keywords)
        
        # Convert to format suitable for word cloud
        keyword_data = []
        for keyword, score in keywords:
            keyword_data.append({
                'text': keyword,
                'weight': float(score),
                'count': len([text for text in review_texts if keyword in text.lower()])
            })
        
        return keyword_data