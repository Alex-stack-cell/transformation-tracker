"""
Centralized configuration management for the McKinsey Transformation Tracker
"""

import os
from dotenv import load_dotenv

# Load environment variables once at module import
load_dotenv()


class Config:
    """Configuration class for environment variables"""

    # API Configuration
    BASE_URL: str = os.getenv("BASE_URL", "http://localhost:8000")

    # App Configuration
    APP_NAME: str = os.getenv("APP_NAME", "mckinsey-transformation-tracker")
    APP_VERSION: str = os.getenv("APP_VERSION", "1.0.0")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")

    # Debugging & Logging
    DEBUG: bool = os.getenv("DEBUG", "True").lower() in ("true", "1", "yes")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "DEBUG")
