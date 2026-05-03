"""
============================================================================
WAFFARHA NEXUS - AUTONOMOUS COMPETITIVE INTELLIGENCE AGENT
============================================================================
Purpose: Production-grade scraping agent with Claude Vision API integration
Features: Instagram/Facebook scraping, AI analysis, Supabase persistence
Architecture: Async, modular, fault-tolerant
============================================================================
"""

import asyncio
import base64
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from uuid import uuid4

import anthropic
from loguru import logger
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from supabase import create_client, Client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from config import Settings, get_settings, setup_directories


# ============================================================================
# CORE AGENT CLASS
# ============================================================================

class CompetitiveIntelligenceAgent:
    """
    Autonomous agent for scraping competitor social media and generating insights
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.supabase: Client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_KEY
        )
        self.claude_client = anthropic.Anthropic(
            api_key=settings.ANTHROPIC_API_KEY
        )
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.session_id = str(uuid4())
        
        # Setup directories
        setup_directories(settings)
        
        # Configure logger
        logger.add(
            settings.LOG_FILE,
            rotation="100 MB",
            retention="30 days",
            level=settings.LOG_LEVEL
        )
        
        logger.info(f"Agent initialized with session ID: {self.session_id}")
    
    # ========================================================================
    # BROWSER MANAGEMENT
    # ========================================================================
    
    async def start_browser(self) -> Browser:
        """Initialize Playwright browser with anti-bot measures"""
        logger.info("Starting browser...")
        
        playwright = await async_playwright().start()
        
        # Browser launch arguments for stealth
        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
        ]
        
        if self.settings.USE_PROXY and self.settings.PROXY_SERVER:
            launch_args.append(f'--proxy-server={self.settings.PROXY_SERVER}')
        
        self.browser = await playwright.chromium.launch(
            headless=self.settings.HEADLESS_MODE,
            args=launch_args
        )
        
        # Create stealth context
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=self.settings.USER_AGENT or self._get_random_user_agent(),
            locale='en-US',
            timezone_id='Africa/Cairo',  # Egyptian timezone
            permissions=['geolocation'],
            geolocation={'latitude': 30.0444, 'longitude': 31.2357},  # Cairo
        )
        
        # Inject anti-detection scripts
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'ar']});
            window.chrome = {runtime: {}};
        """)
        
        logger.info("Browser started successfully")
        return self.browser
    
    async def load_cookies(self, page: Page, platform: str):
        """Load saved session cookies for Instagram or Facebook"""
        cookies_b64 = (
            self.settings.INSTAGRAM_COOKIES if platform == 'instagram'
            else self.settings.FACEBOOK_COOKIES
        )
        
        if not cookies_b64:
            logger.warning(f"No cookies configured for {platform}")
            return
        
        try:
            cookies_json = base64.b64decode(cookies_b64).decode('utf-8')
            cookies = json.loads(cookies_json)
            await self.context.add_cookies(cookies)
            logger.info(f"Loaded {len(cookies)} cookies for {platform}")
        except Exception as e:
            logger.error(f"Failed to load cookies for {platform}: {e}")
    
    def _get_random_user_agent(self) -> str:
        """Generate realistic user agent"""
        from fake_useragent import UserAgent
        ua = UserAgent()
        return ua.chrome
    
    # ========================================================================
    # DATABASE OPERATIONS
    # ========================================================================
    
    def get_competitors_for_scraping(self) -> List[Dict]:
        """Fetch active competitors that need scraping"""
        logger.info("Fetching competitors for scraping...")
        
        result = self.supabase.rpc('get_competitors_for_scraping').execute()
        
        competitors = result.data if result.data else []
        logger.info(f"Found {len(competitors)} competitors ready for scraping")
        
        return competitors
    
    def save_metrics_history(self, competitor_id: str, metrics: Dict):
        """Save competitor metrics to history table"""
        data = {
            'competitor_id': competitor_id,
            'snapshot_timestamp': datetime.now(timezone.utc).isoformat(),
            'scraping_session_id': self.session_id,
            **metrics
        }
        
        self.supabase.table('competitor_metrics_history').insert(data).execute()
        logger.debug(f"Saved metrics for competitor {competitor_id}")
    
    def upload_to_supabase_storage(self, local_path: str, bucket_name: str = "merchant_news_media") -> Optional[str]:
        """Upload a local file to Supabase Storage and return the public URL"""
        try:
            path = Path(local_path)
            if not path.exists():
                logger.warning(f"File not found for upload: {local_path}")
                return None
            
            # Generate a clean remote path: year/month/uuid_filename
            now = datetime.now()
            remote_path = f"{now.year}/{now.month:02d}/{uuid4()}_{path.name}"
            
            with open(local_path, 'rb') as f:
                # Use standard upload
                self.supabase.storage.from_(bucket_name).upload(
                    path=remote_path,
                    file=f.read(),
                    file_options={"content-type": "image/png" if path.suffix == ".png" else "application/octet-stream"}
                )
            
            # Get public URL
            public_url = self.supabase.storage.from_(bucket_name).get_public_url(remote_path)
            logger.debug(f"Uploaded {local_path} to storage: {public_url}")
            return public_url
            
        except Exception as e:
            logger.error(f"Failed to upload to Supabase Storage: {e}")
            return None

    def save_raw_content(self, competitor_id: str, content: Dict) -> str:
        """Save scraped content to database, uploading media to storage first"""
        # Upload screenshots to Supabase Storage
        if 'screenshot_urls' in content:
            public_screenshots = []
            for local_path in content['screenshot_urls']:
                if isinstance(local_path, str) and (local_path.startswith('/') or 'data/' in local_path):
                    public_url = self.upload_to_supabase_storage(local_path)
                    if public_url:
                        public_screenshots.append(public_url)
                    else:
                        public_screenshots.append(local_path) # Fallback
                else:
                    public_screenshots.append(local_path)
            content['screenshot_urls'] = public_screenshots

        # Upload video frames/videos to Supabase Storage
        if 'video_urls' in content:
            public_videos = []
            for local_path in content['video_urls']:
                if isinstance(local_path, str) and (local_path.startswith('/') or 'data/' in local_path):
                    public_url = self.upload_to_supabase_storage(local_path)
                    if public_url:
                        public_videos.append(public_url)
                    else:
                        public_videos.append(local_path) # Fallback
                else:
                    public_videos.append(local_path)
            content['video_urls'] = public_videos

        data = {
            'id': str(uuid4()),
            'competitor_id': competitor_id,
            'scraping_session_id': self.session_id,
            'scraped_at': datetime.now(timezone.utc).isoformat(),
            **content
        }
        
        result = self.supabase.table('competitor_content_raw').insert(data).execute()
        content_id = result.data[0]['id']
        
        logger.debug(f"Saved raw content {content_id}")
        return content_id
    
    def save_strategic_insights(self, competitor_id: str, content_id: str, insights: Dict):
        """Save Claude-generated insights to database"""
        data = {
            'competitor_id': competitor_id,
            'content_id': content_id,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            **insights
        }
        
        self.supabase.table('competitor_strategic_insights').insert(data).execute()
        logger.info(f"Saved strategic insights for competitor {competitor_id}")
    
    def log_scraping_job(self, competitor_id: Optional[str], status: str, 
                         metrics: Dict = None):
        """Log scraping job for observability"""
        data = {
            'id': self.session_id,
            'competitor_id': competitor_id,
            'status': status,
            'started_at': datetime.now(timezone.utc).isoformat(),
            **(metrics or {})
        }
        
        self.supabase.table('scraping_jobs_log').upsert(data).execute()
    
    # ========================================================================
    # INSTAGRAM SCRAPING
    # ========================================================================
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception)
    )
    async def scrape_instagram_profile(self, username: str) -> Dict:
        """
        Scrape Instagram profile with comprehensive data extraction
        Returns: Dict with follower_count, posts, stories, reels
        """
        logger.info(f"Scraping Instagram: @{username}")
        
        page = await self.context.new_page()
        await self.load_cookies(page, 'instagram')
        
        try:
            # Navigate to profile
            url = f"https://www.instagram.com/{username}/"
            await page.goto(url, wait_until='domcontentloaded', timeout=self.settings.PAGE_LOAD_TIMEOUT)
            
            # Random human-like delay
            await self._random_delay()
            
            # Handle any login popups or modals
            await self._handle_instagram_popups(page)
            
            # Extract profile metrics
            metrics = await self._extract_instagram_metrics(page)
            
            # Extract posts
            posts = await self._extract_instagram_posts(page)
            
            # Extract stories (if available)
            stories = await self._extract_instagram_stories(page, username)
            
            # Extract reels
            reels = await self._extract_instagram_reels(page, username)
            
            logger.success(f"Successfully scraped @{username}: {len(posts)} posts, {len(stories)} stories, {len(reels)} reels")
            
            return {
                'metrics': metrics,
                'posts': posts,
                'stories': stories,
                'reels': reels
            }
        
        except Exception as e:
            logger.error(f"Failed to scrape Instagram @{username}: {e}")
            raise
        
        finally:
            await page.close()
    
    async def _handle_instagram_popups(self, page: Page):
        """Detect and close common Instagram login/signup popups"""
        popups = [
            'button:has-text("Not Now")',
            'div[role="dialog"] button:has-text("Close")',
            'svg[aria-label="Close"]',
            'button:has-text("Log In")', # Sometimes closing this works
            'div._abn2 button' # General selector for some modal close buttons
        ]
        
        for selector in popups:
            try:
                element = page.locator(selector).first
                if await element.is_visible(timeout=2000):
                    await element.click()
                    logger.info(f"Closed Instagram popup: {selector}")
                    await asyncio.sleep(1)
            except:
                continue

    async def _extract_instagram_metrics(self, page: Page) -> Dict:
        """Extract follower count, following, posts count"""
        try:
            # Wait for profile header to load
            await page.wait_for_selector('header section', timeout=10000)
            
            # Extract metrics using multiple selectors (Instagram's DOM changes frequently)
            metrics = {}
            
            # Fallback 1: Meta description
            meta_content = await page.locator('meta[property="og:description"]').get_attribute('content', timeout=2000)
            if meta_content:
                f_match = re.search(r'([\d.,KMBkmb]+)\s+Followers', meta_content, re.IGNORECASE)
                if f_match:
                    metrics['ig_followers_count'] = self._parse_count(f_match.group(1))
                    
            # Fallback 2: Profile stats list (more specific selectors)
            if not metrics.get('ig_followers_count'):
                # Instagram followers usually have a title attribute with the full number
                followers_el = page.locator('header section ul li').filter(has_text='followers')
                title_attr = await followers_el.locator('span[title]').get_attribute('title')
                if title_attr:
                    metrics['ig_followers_count'] = self._parse_count(title_attr)
                else:
                    text = await followers_el.inner_text()
                    metrics['ig_followers_count'] = self._parse_count(text)

            # Extract others
            stats = await page.locator('header section ul li').all_text_contents()
            for stat in stats:
                if 'following' in stat.lower() and 'ig_following_count' not in metrics:
                    metrics['ig_following_count'] = self._parse_count(stat)
                elif 'posts' in stat.lower() and 'ig_posts_count' not in metrics:
                    metrics['ig_posts_count'] = self._parse_count(stat)
            
            # Sanity check: If followers are extremely low (e.g. < 500) for a brand like Pizza Hut, 
            # it might be a bot check or incorrect element. Try a different selector.
            if metrics.get('ig_followers_count', 0) < 500:
                logger.warning(f"Detected suspicious low follower count ({metrics['ig_followers_count']}) for {page.url}")
                # Try to find the large number in the header
                all_header_spans = await page.locator('header span').all_inner_texts()
                for span in all_header_spans:
                    if any(x in span for x in ['K', 'M', 'B', ',']):
                        val = self._parse_count(span)
                        if val > metrics['ig_followers_count']:
                            metrics['ig_followers_count'] = val
                            logger.info(f"Corrected follower count to {val} using fallback span")
                            break
            
            logger.debug(f"Extracted Instagram metrics: {metrics}")
            return metrics
        
        except Exception as e:
            logger.warning(f"Failed to extract Instagram metrics: {e}")
            return {}
    
    async def _extract_instagram_posts(self, page: Page) -> List[Dict]:
        """Extract latest posts with captions and engagement"""
        posts = []
        
        try:
            # Scroll to load posts
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(2)
            
            # Find post links - Using more generic selectors as fallback
            post_links = await page.locator('a[href*="/p/"]').all()
            if not post_links:
                post_links = await page.locator('article a').all()
                post_links = [l for l in post_links if '/p/' in (await l.get_attribute('href') or '')]
            
            for i, link in enumerate(post_links[:self.settings.MAX_POSTS_PER_SCRAPE]):
                try:
                    href = await link.get_attribute('href')
                    post_url = f"https://www.instagram.com{href}"
                    
                    # Open post in new page
                    post_page = await self.context.new_page()
                    await post_page.goto(post_url, wait_until='domcontentloaded')
                    await self._random_delay(0.5, 2.0)
                    
                    # Extract post data
                    post_data = await self._extract_post_details(post_page, post_url)
                    posts.append(post_data)
                    
                    await post_page.close()
                
                except Exception as e:
                    logger.warning(f"Failed to extract post {i+1}: {e}")
                    continue
            
            logger.info(f"Extracted {len(posts)} Instagram posts")
            return posts
        
        except Exception as e:
            logger.error(f"Failed to extract Instagram posts: {e}")
            return []
    
    async def _extract_post_details(self, page: Page, post_url: str) -> Dict:
        """Extract detailed post information"""
        # Take screenshot
        screenshot_path = self.settings.SCREENSHOTS_DIR / f"post_{uuid4()}.png"
        await page.screenshot(path=screenshot_path, full_page=False)
        
        # Extract caption
        caption = ""
        try:
            caption_element = await page.locator('h1').first.inner_text(timeout=2000)
            caption = caption_element.strip()
        except:
            pass
        
        # Extract engagement metrics
        likes = await self._extract_likes_count(page)
        comments = await self._extract_comments_count(page)
        
        # Extract media URLs
        media_urls = []
        try:
            imgs = await page.locator('article img[src*="instagram"]').all()
            for img in imgs[:5]:  # Max 5 images
                src = await img.get_attribute('src')
                if src:
                    media_urls.append(src)
        except:
            pass
        
        # Extract hashtags
        hashtags = re.findall(r'#\w+', caption)
        
        return {
            'content_type': 'post',
            'platform': 'instagram',
            'post_url': post_url,
            'caption': caption,
            'hashtags': hashtags,
            'likes_count': likes,
            'comments_count': comments,
            'media_urls': media_urls,
            'screenshot_urls': [str(screenshot_path)],
            'posted_at': datetime.now(timezone.utc).isoformat(),  # Approximate
        }
    
    async def _extract_instagram_stories(self, page: Page, username: str) -> List[Dict]:
        """Extract active stories"""
        stories = []
        
        try:
            # Check if stories ring exists
            story_ring = page.locator(f'canvas[aria-label*="{username}"]').first
            
            if await story_ring.is_visible():
                await story_ring.click()
                await asyncio.sleep(2)
                
                # Extract stories
                for i in range(self.settings.MAX_STORIES_PER_SCRAPE):
                    try:
                        # Take screenshot
                        screenshot_path = self.settings.SCREENSHOTS_DIR / f"story_{uuid4()}.png"
                        await page.screenshot(path=screenshot_path)
                        
                        stories.append({
                            'content_type': 'story',
                            'platform': 'instagram',
                            'screenshot_urls': [str(screenshot_path)],
                            'posted_at': datetime.now(timezone.utc).isoformat(),
                        })
                        
                        # Try to go to next story
                        next_button = page.locator('button[aria-label="Next"]').first
                        if await next_button.is_visible():
                            await next_button.click()
                            await asyncio.sleep(2)
                        else:
                            break
                    
                    except Exception as e:
                        logger.debug(f"No more stories: {e}")
                        break
            
            logger.info(f"Extracted {len(stories)} Instagram stories")
            return stories
        
        except Exception as e:
            logger.warning(f"Failed to extract Instagram stories: {e}")
            return []
    
    async def _extract_instagram_reels(self, page: Page, username: str) -> List[Dict]:
        """Extract reels with frame sampling"""
        reels = []
        
        try:
            # Navigate directly to Reels page
            reels_url = f"https://www.instagram.com/{username}/reels/"
            await page.goto(reels_url, wait_until='domcontentloaded')
            await asyncio.sleep(5)
            
            # Find reel links
            reel_links = await page.locator('a[href*="/reel/"]').all()
            
            for i, link in enumerate(reel_links[:self.settings.MAX_REELS_PER_SCRAPE]):
                    try:
                        href = await link.get_attribute('href')
                        reel_url = f"https://www.instagram.com{href}"
                        
                        # Open reel
                        reel_page = await self.context.new_page()
                        await reel_page.goto(reel_url, wait_until='domcontentloaded')
                        await asyncio.sleep(3)
                        
                        # Sample video frames
                        frames = await self._sample_video_frames(reel_page)
                        
                        reels.append({
                            'content_type': 'reel',
                            'platform': 'instagram',
                            'post_url': reel_url,
                            'video_frame_urls': frames,
                            'posted_at': datetime.now(timezone.utc).isoformat(),
                        })
                        
                        await reel_page.close()
                    
                    except Exception as e:
                        logger.warning(f"Failed to extract reel {i+1}: {e}")
                        continue
            logger.info(f"Extracted {len(reels)} Instagram reels")
            return reels
        
        except Exception as e:
            logger.warning(f"Failed to extract Instagram reels: {e}")
            return []
    
    async def _sample_video_frames(self, page: Page) -> List[str]:
        """Sample frames from video at regular intervals"""
        frames = []
        
        try:
            # Play video using JS to bypass UI overlays intercepting clicks
            video = page.locator('video').first
            await video.evaluate('el => el.play()')
            await asyncio.sleep(1)
            
            # Get video duration
            duration = await video.evaluate('el => el.duration')
            
            if duration > 0:
                sample_times = [
                    i * self.settings.REEL_FRAME_SAMPLE_RATE 
                    for i in range(min(int(duration / self.settings.REEL_FRAME_SAMPLE_RATE) + 1, self.settings.MAX_FRAMES_PER_REEL))
                ]
                
                for sample_time in sample_times:
                    # Seek to time
                    await video.evaluate(f'el => el.currentTime = {sample_time}')
                    await asyncio.sleep(0.5)
                    
                    # Capture frame
                    frame_path = self.settings.VIDEO_FRAMES_DIR / f"frame_{uuid4()}.png"
                    await page.screenshot(path=frame_path)
                    frames.append(str(frame_path))
        
        except Exception as e:
            logger.warning(f"Failed to sample video frames: {e}")
        
        return frames
    
    # ========================================================================
    # FACEBOOK SCRAPING (Similar pattern to Instagram)
    # ========================================================================
    
    async def scrape_facebook_page(self, page_url: str) -> Dict:
        """
        Scrape Facebook business page
        Note: Simplified implementation - expand based on specific needs
        """
        logger.info(f"Scraping Facebook: {page_url}")
        
        page = await self.context.new_page()
        await self.load_cookies(page, 'facebook')
        
        try:
            # Revert to networkidle for Facebook to ensure background scripts run
            await page.goto(page_url, wait_until='networkidle', timeout=self.settings.PAGE_LOAD_TIMEOUT)
            
            # Intensive "Human" settle time
            logger.info("Waiting for Facebook content to settle...")
            await asyncio.sleep(8)
            
            # Active Scrolling "Jiggle" to trigger lazy loading
            for i in range(2):
                logger.debug(f"Triggering lazy load scroll {i+1}...")
                await page.evaluate('window.scrollTo(0, 1500)')
                await asyncio.sleep(2)
                await page.evaluate('window.scrollTo(0, 500)')
                await asyncio.sleep(1)
            
            # Extract page metrics (likes and followers)
            metrics = {}
            try:
                # Scroll a bit to trigger lazy loading of some elements
                await page.evaluate('window.scrollTo(0, 500)')
                await asyncio.sleep(2)
                
                # Try multiple common selectors for Facebook likes/followers
                selectors = [
                    'a[href*="followers"]',
                    'a[href*="likes"]',
                    'span:has-text("likes")',
                    'span:has-text("followers")'
                ]
                
                for selector in selectors:
                    try:
                        element = page.locator(selector).first
                        if await element.is_visible(timeout=2000):
                            text = await element.inner_text()
                            if 'likes' in text.lower():
                                metrics['fb_page_likes'] = self._parse_count(text)
                            elif 'followers' in text.lower():
                                metrics['fb_followers_count'] = self._parse_count(text)
                    except:
                        continue
            except Exception as e:
                logger.debug(f"Metrics extraction error: {e}")
            
            # Extract recent posts
            posts = []
            
            # Try multiple selectors for Facebook posts, prioritizing aria-posinset which marks main feed items
            post_selectors = [
                'div[aria-posinset]', # Most reliable selector for modern Facebook posts
                'div[data-testid="post_container"]',
                'div[role="main"] div[role="article"]',
                'div.x1yztbdb.x1n2onr6.xh8yej3.x1ja2u2z'
            ]
            
            post_elements = []
            for selector in post_selectors:
                elements = await page.locator(selector).all()
                # Filter to only keep elements that look like a full post (usually larger)
                valid_posts = []
                for el in elements:
                    try:
                        # Posts usually have a 'Share' or 'Like' button or a specific structure
                        box = await el.bounding_box()
                        if box and box['height'] > 200: # Comments are usually shorter
                            valid_posts.append(el)
                    except:
                        continue
                
                if len(valid_posts) > 0:
                    post_elements = valid_posts
                    logger.debug(f"Found {len(valid_posts)} valid posts using selector: {selector}")
                    break
            
            if not post_elements:
                # Final fallback: look for anything that looks like a post message
                post_elements = await page.locator('div[data-ad-comet-preview="message"]').all()
                if post_elements:
                    logger.debug(f"Found {len(post_elements)} posts using ad-comet-preview selector")
            
            for i, post_el in enumerate(post_elements[:self.settings.MAX_POSTS_PER_SCRAPE]):
                try:
                    # Scroll post into view to trigger loading
                    await post_el.scroll_into_view_if_needed()
                    
                    # Wait for skeletons to disappear - Facebook skeletons often have 'display: none' or are replaced
                    # A good marker is the "Like" or "Comment" button
                    try:
                        # First wait for a button that indicates the post is interactive
                        await post_el.locator('div[aria-label="Like"], div[aria-label="Comment"], i[style*="background-image"]').first.wait_for(state='visible', timeout=10000)
                        
                        # Wait for any text to appear (skeletons usually have 0 text or very little)
                        await page.wait_for_function(
                            'el => el.innerText.length > 20', 
                            arg=post_el, 
                            timeout=5000
                        )
                    except:
                        # Fallback if the verification fails, wait longer
                        logger.debug(f"Post {i+1} verification timed out, waiting 5s fallback...")
                        await asyncio.sleep(5)
                    
                    # Small extra buffer for high-res images to render
                    await asyncio.sleep(1)
                    
                    screenshot_path = self.settings.SCREENSHOTS_DIR / f"fb_post_{uuid4()}.png"
                    await post_el.screenshot(path=screenshot_path)
                    
                    text = await post_el.inner_text()
                    
                    posts.append({
                        'content_type': 'post',
                        'platform': 'facebook',
                        'caption': text,
                        'screenshot_urls': [str(screenshot_path)],
                        'posted_at': datetime.now(timezone.utc).isoformat(),
                    })
                
                except Exception as e:
                    logger.warning(f"Failed to extract FB post {i+1}: {e}")
                    continue
            
            # Extract reels
            base_url = page_url.split('?')[0].rstrip('/')
            reels = await self._extract_facebook_reels(base_url)
            
            logger.success(f"Successfully scraped Facebook page: {len(posts)} posts, {len(reels)} reels")
            
            return {
                'metrics': metrics,
                'posts': posts,
                'stories': [],  # Facebook Stories require different approach
                'reels': reels
            }
        
        except Exception as e:
            logger.error(f"Failed to scrape Facebook page: {e}")
            raise
        
        finally:
            await page.close()
            
    async def _extract_facebook_reels(self, base_url: str) -> List[Dict]:
        """Extract reels from Facebook page"""
        reels = []
        try:
            reel_page = await self.context.new_page()
            reels_url = f"{base_url}/reels/"
            
            logger.info(f"Extracting FB reels from: {reels_url}")
            await reel_page.goto(reels_url, wait_until='domcontentloaded')
            await asyncio.sleep(5)
            
            # Find reel links
            reel_links = await reel_page.locator('a[href*="/reel/"], a[href*="/video.php"]').all()
            
            for i, link in enumerate(reel_links[:self.settings.MAX_REELS_PER_SCRAPE]):
                try:
                    href = await link.get_attribute('href')
                    
                    # Filter out ui tab links
                    if not href or href == '/reel/?s=tab' or '?' in href and 's=tab' in href:
                        continue
                        
                    reel_url = href if href.startswith('http') else f"https://www.facebook.com{href}"
                    
                    # Save the URL without heavy sampling
                    reels.append({
                        'content_type': 'reel',
                        'platform': 'facebook',
                        'post_url': reel_url,
                        'posted_at': datetime.now(timezone.utc).isoformat(),
                    })
                    
                except Exception as e:
                    logger.warning(f"Failed to extract FB reel {i+1}: {e}")
                    continue
                    
            await reel_page.close()
            logger.info(f"Extracted {len(reels)} Facebook reels")
            return reels
            
        except Exception as e:
            logger.warning(f"Failed to extract Facebook reels: {e}")
            if 'reel_page' in locals():
                await reel_page.close()
            return []
    # ========================================================================
    # AI ANALYSIS WITH CLAUDE VISION
    # ========================================================================
    
    async def analyze_with_claude(self, competitor_name: str, scraped_data: Dict) -> Dict:
        """
        Send scraped content to Claude Vision API for strategic analysis
        """
        logger.info(f"Analyzing {competitor_name} with Claude Vision API...")
        
        # Prepare images for analysis
        image_contents = []
        
        # Collect all screenshots and frames
        all_images = []
        for content_type in ['posts', 'stories', 'reels']:
            for item in scraped_data.get(content_type, []):
                all_images.extend(item.get('screenshot_urls', []))
                all_images.extend(item.get('video_frame_urls', []))
        
        # Convert images to base64
        import httpx
        for img_path in all_images[:20]:  # Max 20 images to avoid token limits
            try:
                if isinstance(img_path, str) and img_path.startswith('http'):
                    # Download from URL
                    with httpx.Client() as client:
                        response = client.get(img_path)
                        if response.status_code == 200:
                            img_bytes = response.content
                        else:
                            logger.warning(f"Failed to download image from {img_path}: {response.status_code}")
                            continue
                else:
                    # Read from local file
                    with open(img_path, 'rb') as f:
                        img_bytes = f.read()
                
                img_data = base64.standard_b64encode(img_bytes).decode('utf-8')
                image_contents.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": img_data
                    }
                })
            except Exception as e:
                logger.warning(f"Failed to encode image {img_path}: {e}")
        
        # Prepare text context
        text_context = self._prepare_text_context(scraped_data)
        
        # Build Claude prompt
        prompt = f"""You are analyzing competitor social media content for Waffarha, an Egyptian e-commerce/discounts platform.

**Competitor:** {competitor_name}

**Your Task:**
Analyze the provided images and text to extract strategic business intelligence focused on:

1. **Pricing Strategy:**
   - Identify any visible prices, discounts, or promotions
   - Calculate discount percentages if possible
   - Detect pricing patterns (e.g., "Buy 1 Get 1", "50% off", bundle deals)

2. **Marketing Strategy:**
   - What is their current marketing focus? (seasonal campaigns, new products, brand positioning)
   - What is the tone and messaging style?
   - Who is their target audience?

3. **Competitive Threats:**
   - What offers or campaigns could threaten Waffarha's market share?
   - Are they launching new products or services?

4. **Opportunities:**
   - What successful tactics could Waffarha adopt or counter?
   - Any gaps in their strategy we could exploit?

**Context from scraped data:**
{text_context}

**Respond in the following JSON structure:**
{{
  "pricing_analysis": {{
    "detected_prices": [{{  "item": "Product name", "original_price": 100, "discount_price": 75, "discount_percentage": 25 }}],
    "promotion_type": "percentage_discount | bundle | bogo | seasonal",
    "pricing_strategy": "aggressive | competitive | premium"
  }},
  "marketing_strategy": {{
    "campaign_focus": "description",
    "tone": "playful | professional | urgent | luxury",
    "target_audience": "youth | families | professionals | all"
  }},
  "competitive_threats": [
    "Threat 1: Description of what makes this threatening",
    "Threat 2: ..."
  ],
  "opportunities": [
    "Opportunity 1: What Waffarha can learn or counter",
    "Opportunity 2: ..."
  ],
  "executive_summary": "2-3 sentence summary for leadership",
  "confidence_score": 0.0 to 1.0,
  "urgency_level": "low | medium | high | critical"
}}

Provide ONLY valid JSON, no markdown formatting."""

        # Call Claude API
        start_time = time.time()
        
        try:
            message = self.claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=self.settings.CLAUDE_MAX_TOKENS,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        *image_contents
                    ]
                }]
            )
            
            processing_time = int((time.time() - start_time) * 1000)
            
            # Parse Claude's response
            response_text = message.content[0].text
            
            # Extract JSON from response (Claude might wrap it in markdown)
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                insights_json = json.loads(json_match.group(0))
            else:
                # Fallback if JSON parsing fails
                insights_json = {
                    "executive_summary": response_text[:500],
                    "detailed_analysis": response_text,
                    "confidence_score": 0.5
                }
            
            # Prepare final insights dict
            insights = {
                'pricing_analysis': insights_json.get('pricing_analysis', {}),
                'promotion_details': insights_json.get('pricing_analysis', {}).get('promotion_type'),
                'marketing_strategy': insights_json.get('marketing_strategy', {}),
                'key_findings': insights_json.get('competitive_threats', [])[:3],
                'competitive_threats': insights_json.get('competitive_threats', []),
                'opportunities': insights_json.get('opportunities', []),
                'executive_summary': insights_json.get('executive_summary', ''),
                'detailed_analysis': response_text,
                'confidence_score': insights_json.get('confidence_score', 0.7),
                'urgency_level': insights_json.get('urgency_level', 'medium'),
                'processing_time_ms': processing_time,
                'token_count': message.usage.output_tokens,
            }
            
            logger.success(f"Claude analysis complete. Tokens: {message.usage.output_tokens}, Time: {processing_time}ms")
            
            return insights
        
        except Exception as e:
            logger.error(f"Claude API error: {e}")
            raise
    
    def _prepare_text_context(self, scraped_data: Dict) -> str:
        """Prepare text summary of scraped data for Claude"""
        context_parts = []
        
        # Add metrics
        metrics = scraped_data.get('metrics', {})
        if metrics:
            context_parts.append(f"**Metrics:**\n- Followers: {metrics.get('ig_followers_count', 'N/A')}\n- Posts: {metrics.get('ig_posts_count', 'N/A')}")
        
        # Add post captions
        posts = scraped_data.get('posts', [])
        if posts:
            captions = [p.get('caption', '')[:200] for p in posts[:5]]
            context_parts.append(f"**Recent Post Captions:**\n" + "\n".join(f"- {c}" for c in captions if c))
        
        return "\n\n".join(context_parts)
    
    # ========================================================================
    # HELPER UTILITIES
    # ========================================================================
    
    async def _random_delay(self, min_delay: float = None, max_delay: float = None):
        """Random delay to mimic human behavior"""
        import random
        min_d = min_delay or self.settings.RANDOM_DELAY_MIN
        max_d = max_delay or self.settings.RANDOM_DELAY_MAX
        delay = random.uniform(min_d, max_d)
        await asyncio.sleep(delay)
    
    def _parse_count(self, text: str) -> int:
        """Parse count from text like '1.2M' or '45K' or '1,234'"""
        text = text.replace(',', '').strip()
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        
        for suffix, multiplier in multipliers.items():
            if suffix in text.upper():
                number = float(text.upper().replace(suffix, ''))
                return int(number * multiplier)
        
        # Try to extract just the number
        match = re.search(r'\d+', text)
        return int(match.group(0)) if match else 0
    
    async def _extract_likes_count(self, page: Page) -> int:
        """Extract likes count from post"""
        try:
            likes_text = await page.locator('section span').filter(has_text='likes').first.inner_text(timeout=2000)
            return self._parse_count(likes_text)
        except:
            return 0
    
    async def _extract_comments_count(self, page: Page) -> int:
        """Extract comments count"""
        try:
            comments = await page.locator('section span').filter(has_text='comments').first.inner_text(timeout=2000)
            return self._parse_count(comments)
        except:
            return 0
    
    # ========================================================================
    # MAIN ORCHESTRATION
    # ========================================================================
    
    async def process_competitor(self, competitor: Dict):
        """Process a single competitor: scrape + analyze + save"""
        competitor_id = competitor['competitor_id']
        competitor_name = competitor['competitor_name']
        
        logger.info(f"Processing competitor: {competitor_name}")
        
        try:
            # Scrape Instagram
            ig_data = {}
            ig_handle = competitor.get('instagram_handle')
            if not ig_handle and competitor.get('instagram_url'):
                # Extract handle from URL: https://www.instagram.com/handle/ -> handle
                match = re.search(r'instagram\.com/([^/?#]+)', competitor['instagram_url'])
                if match:
                    ig_handle = match.group(1)
            
            if ig_handle:
                ig_data = await self.scrape_instagram_profile(ig_handle)
            
            # Scrape Facebook
            fb_data = {}
            if competitor.get('facebook_url'):
                fb_data = await self.scrape_facebook_page(competitor['facebook_url'])
            
            # Merge data
            all_data = {
                'metrics': {**ig_data.get('metrics', {}), **fb_data.get('metrics', {})},
                'posts': ig_data.get('posts', []) + fb_data.get('posts', []),
                'stories': ig_data.get('stories', []),
                'reels': ig_data.get('reels', [])
            }
            
            # Calculate averages for Instagram
            ig_posts = ig_data.get('posts', [])
            if ig_posts:
                avg_likes = sum(p.get('likes_count', 0) for p in ig_posts) / len(ig_posts)
                avg_comments = sum(p.get('comments_count', 0) for p in ig_posts) / len(ig_posts)
                
                all_data['metrics']['ig_avg_likes'] = avg_likes
                all_data['metrics']['ig_avg_comments'] = avg_comments
                
                followers = all_data['metrics'].get('ig_followers_count', 0)
                if followers > 0:
                    engagement = ((avg_likes + avg_comments) / followers) * 100
                    all_data['metrics']['ig_engagement_rate'] = engagement
            
            # Save metrics history
            if all_data['metrics']:
                self.save_metrics_history(competitor_id, all_data['metrics'])
            
            # Save raw content
            content_ids = []
            for content_item in (all_data['posts'] + all_data['stories'] + all_data['reels']):
                content_id = self.save_raw_content(competitor_id, content_item)
                content_ids.append(content_id)
            
            # AI Analysis
            if self.settings.ENABLE_AI_ANALYSIS and content_ids:
                insights = await self.analyze_with_claude(competitor_name, all_data)
                
                # Save insights (link to first content item as representative)
                self.save_strategic_insights(
                    competitor_id=competitor_id,
                    content_id=content_ids[0],
                    insights=insights
                )
            
            logger.success(f"Completed processing: {competitor_name}")
            
            # UPDATE DIRECTORY STATUS
            try:
                update_data = {
                    'last_scraped_at': datetime.now(timezone.utc).isoformat(),
                    'last_metrics_update': datetime.now(timezone.utc).isoformat()
                }
                # Also save current metrics to directory for fast access if columns exist
                if 'ig_followers_count' in all_data['metrics']:
                    update_data['current_ig_followers'] = all_data['metrics']['ig_followers_count']
                
                self.supabase.table('competitors_directory').update(update_data).eq('id', competitor_id).execute()
                logger.info(f"Updated directory status for {competitor_name}")
            except Exception as e:
                logger.warning(f"Could not update directory timestamp: {e}")
            
        except Exception as e:
            logger.error(f"Failed to process {competitor_name}: {e}")
            raise
    
    async def run(self):
        """Main execution loop"""
        logger.info("Starting Competitive Intelligence Agent...")
        
        try:
            # Start browser
            await self.start_browser()
            
            # Get competitors
            competitors = self.get_competitors_for_scraping()
            
            if not competitors:
                logger.warning("No competitors to scrape")
                return
            
            # Log job start
            self.log_scraping_job(None, 'running')
            
            # Process competitors (with concurrency limit)
            tasks = [self.process_competitor(comp) for comp in competitors]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes/failures
            successes = sum(1 for r in results if not isinstance(r, Exception))
            failures = len(results) - successes
            
            # Log job completion
            self.log_scraping_job(
                None,
                'completed' if failures == 0 else 'partial',
                {
                    'completed_at': datetime.now(timezone.utc).isoformat(),
                    'items_scraped': successes,
                    'errors_count': failures
                }
            )
            
            logger.info(f"Agent run complete. Successes: {successes}, Failures: {failures}")
        
        except Exception as e:
            logger.critical(f"Agent run failed: {e}")
            self.log_scraping_job(None, 'failed', {
                'error_messages': {'error': str(e)}
            })
        
        finally:
            if self.browser:
                await self.browser.close()
                logger.info("Browser closed")


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main():
    """Main entry point"""
    settings = get_settings()
    agent = CompetitiveIntelligenceAgent(settings)
    await agent.run()


if __name__ == "__main__":
    asyncio.run(main())
