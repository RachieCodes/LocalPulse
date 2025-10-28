from celery import Celery
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Celery configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Create Celery app
app = Celery('localpulse')

# Configure Celery
app.conf.update(
    broker_url=REDIS_URL,
    result_backend=REDIS_URL,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task routing
    task_routes={
        'scheduler.tasks.scrape_businesses': {'queue': 'scraping'},
        'scheduler.tasks.process_reviews': {'queue': 'processing'},
        'scheduler.tasks.update_analytics': {'queue': 'analytics'},
    },
    
    # Beat schedule for periodic tasks
    beat_schedule={
        'scrape-yelp-daily': {
            'task': 'scheduler.tasks.scrape_yelp_businesses',
            'schedule': timedelta(hours=24),  # Daily
            'args': ('restaurants', 'New York, NY', 50)
        },
        'scrape-directories-weekly': {
            'task': 'scheduler.tasks.scrape_directory_businesses',
            'schedule': timedelta(days=7),  # Weekly
            'args': ('restaurants', 'New York, NY', 30)
        },
        'process-reviews-hourly': {
            'task': 'scheduler.tasks.process_new_reviews',
            'schedule': timedelta(hours=1),  # Hourly
            'args': (200,)
        },
        'update-analytics-daily': {
            'task': 'scheduler.tasks.update_business_analytics',
            'schedule': timedelta(hours=12),  # Twice daily
        },
        'generate-trending-keywords': {
            'task': 'scheduler.tasks.generate_trending_keywords',
            'schedule': timedelta(hours=6),  # Every 6 hours
            'args': (7,)  # Last 7 days
        },
        'detect-rating-anomalies': {
            'task': 'scheduler.tasks.detect_rating_anomalies',
            'schedule': timedelta(hours=24),  # Daily
        },
    },
    
    # Worker configuration
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
)