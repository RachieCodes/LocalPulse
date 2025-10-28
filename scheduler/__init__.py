# Scheduler package for LocalPulse

from .celery_app import app as celery_app
from . import tasks

__all__ = ['celery_app', 'tasks']