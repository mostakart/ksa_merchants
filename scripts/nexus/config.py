"""
============================================================================
WAFFARHA NEXUS - CONFIGURATION MODULE
============================================================================
Purpose: Centralized configuration management with environment variables
Security: All secrets loaded from .env file
============================================================================
"""

import os
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # ========================================================================
    # Supabase Configuration
    # ========================================================================
    SUPABASE_URL: str
    SUPABASE_KEY: str  # Service role key for full access
    
    # ========================================================================
    # Anthropic Claude API
    # ========================================================================
    ANTHROPIC_API_KEY: str
    CLAUDE_MODEL: str = "claude-3-5-sonnet-20241022"
    CLAUDE_MAX_TOKENS: int = 4096
    
    # ========================================================================
    # Scraping Configuration
    # ========================================================================
    # Session Cookies (Base64 encoded JSON for Instagram/Facebook)
    INSTAGRAM_COOKIES: Optional[str] = None
    FACEBOOK_COOKIES: Optional[str] = None
    
    # Browser Configuration
    HEADLESS_MODE: bool = True
    BROWSER_TIMEOUT: int = 60000  # 60 seconds
    PAGE_LOAD_TIMEOUT: int = 30000  # 30 seconds
    
    # Anti-Bot Measures
    USE_PROXY: bool = False
    PROXY_SERVER: Optional[str] = None  # Format: http://user:pass@host:port
    RANDOM_DELAY_MIN: float = 2.0  # Minimum delay between actions (seconds)
    RANDOM_DELAY_MAX: float = 5.0  # Maximum delay
    
    # User Agent Rotation
    USER_AGENT: Optional[str] = None  # If None, will use fake-useragent
    
    # ========================================================================
    # Content Extraction Settings
    # ========================================================================
    MAX_POSTS_PER_SCRAPE: int = 20
    MAX_STORIES_PER_SCRAPE: int = 10
    MAX_REELS_PER_SCRAPE: int = 5
    
    # Video Frame Sampling
    REEL_FRAME_SAMPLE_RATE: int = 3  # Extract 1 frame every N seconds
    MAX_FRAMES_PER_REEL: int = 5
    
    # ========================================================================
    # Processing Configuration
    # ========================================================================
    BATCH_SIZE: int = 5  # Process N competitors concurrently
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5  # Seconds between retries
    
    # AI Analysis
    ENABLE_AI_ANALYSIS: bool = True
    AI_ANALYSIS_CONFIDENCE_THRESHOLD: float = 0.7
    
    # ========================================================================
    # Storage & Caching
    # ========================================================================
    SCREENSHOTS_DIR: Path = Path("./data/screenshots")
    VIDEO_FRAMES_DIR: Path = Path("./data/video_frames")
    CACHE_DIR: Path = Path("./data/cache")
    
    # ========================================================================
    # Logging & Monitoring
    # ========================================================================
    LOG_LEVEL: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    LOG_FILE: Path = Path("./logs/nexus_agent.log")
    
    # Sentry (optional)
    SENTRY_DSN: Optional[str] = None
    ENABLE_SENTRY: bool = False
    
    # ========================================================================
    # Scheduling
    # ========================================================================
    RUN_MODE: str = "once"  # "once", "continuous", "scheduled"
    CONTINUOUS_INTERVAL_MINUTES: int = 60
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# ============================================================================
# Initialize Settings
# ============================================================================

def get_settings() -> Settings:
    """Load and validate settings"""
    return Settings()


# ============================================================================
# Directory Setup
# ============================================================================

def setup_directories(settings: Settings):
    """Create necessary directories if they don't exist"""
    directories = [
        settings.SCREENSHOTS_DIR,
        settings.VIDEO_FRAMES_DIR,
        settings.CACHE_DIR,
        settings.LOG_FILE.parent,
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


# ============================================================================
# Example .env File Template
# ============================================================================

ENV_TEMPLATE = """
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key-here

# Anthropic Claude
ANTHROPIC_API_KEY=sk-ant-your-api-key-here
CLAUDE_MODEL=claude-3-5-sonnet-20241022

# Instagram Cookies (Base64 encoded JSON array)
# Generate via: browser DevTools > Application > Cookies > Export as JSON > Base64 encode
INSTAGRAM_COOKIES=W3sibmFtZSI6InNlc3Npb25pZCIsInZhbHVlIjoieW91cl9zZXNzaW9uX2hlcmUifV0=

# Facebook Cookies (Base64 encoded JSON array)
FACEBOOK_COOKIES=W3sibmFtZSI6ImNfdXNlciIsInZhbHVlIjoieW91cl91c2VyX2lkIn1d

# Browser Settings
HEADLESS_MODE=true
USE_PROXY=false
# PROXY_SERVER=http://user:pass@proxy.example.com:8080

# Scraping Limits
MAX_POSTS_PER_SCRAPE=20
MAX_STORIES_PER_SCRAPE=10
MAX_REELS_PER_SCRAPE=5

# Processing
BATCH_SIZE=3
LOG_LEVEL=INFO

# Scheduling
RUN_MODE=once
# RUN_MODE=continuous
# CONTINUOUS_INTERVAL_MINUTES=60
"""


if __name__ == "__main__":
    # Print example .env template
    print(ENV_TEMPLATE)
