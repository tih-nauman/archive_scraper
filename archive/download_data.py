import requests
from bs4 import BeautifulSoup
import os
import logging
import time
import random
import asyncio
import aiohttp
import json
from aiofiles import open as aio_open
from tenacity import retry, stop_after_attempt, wait_exponential
from aiohttp_socks import ProxyConnector
from datetime import datetime
from urllib.parse import urljoin
import hashlib
from tqdm.asyncio import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("archive_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("archive_scraper")

# Redis for distributed job tracking (optional)
REDIS_AVAILABLE = False
logger.warning("Redis not available. Using local job tracking.")

# Configuration
CONFIG = {
    "base_url": "https://archive.org",
    "output_folder": "downloads",
    "metadata_folder": "metadata",
    "file_format": "PDF",
    "concurrent_downloads": 30,  # Per worker
    "concurrent_items": 60,     # Number of items to process simultaneously
    "max_retries": 5,
    "min_delay": 1,            # Minimum delay between requests in seconds
    "max_delay": 5,            # Maximum delay between requests in seconds
    "proxy_list_file": "proxies.txt",  # File containing proxy URLs (optional)
    "user_agents_file": "user_agents.txt",  # File containing user agents (optional)
    "batch_size": 300,         # Number of items to process in a batch
    "resume_downloads": True,  # Resume interrupted downloads
}

CONFIG.update({
    "retry_delay": 120,  # 2 minutes delay for rate limiting
    "max_attempts": 3,   # Maximum retry attempts
    "failed_urls_file": "failed_urls.txt",  # File to store failed URLs
    "zyte_api_key": "279062e2a8854f20822b72dc2293d6ef"  # Added Zyte API key
})

# Load proxies if available
PROXIES = []
try:
    if os.path.exists(CONFIG["proxy_list_file"]):
        with open(CONFIG["proxy_list_file"], "r") as f:
            PROXIES = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(PROXIES)} proxies")
except Exception as e:
    logger.warning(f"Failed to load proxies: {e}")

# Zyte Proxy Configuration
ZYTE_PROXY_URL = f"http://{CONFIG['zyte_api_key']}:@proxy.zyte.com:8011"
logger.info("Zyte proxy configured")

# Load user agents if available
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
]
try:
    if os.path.exists(CONFIG["user_agents_file"]):
        with open(CONFIG["user_agents_file"], "r") as f:
            USER_AGENTS = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(USER_AGENTS)} user agents")
except Exception as e:
    logger.warning(f"Failed to load user agents: {e}")

# Create necessary folders
os.makedirs(CONFIG["output_folder"], exist_ok=True)
os.makedirs(CONFIG["metadata_folder"], exist_ok=True)

def get_random_proxy():
    """Return a random proxy from the list if available."""
    return random.choice(PROXIES) if PROXIES else None

def get_random_user_agent():
    """Return a random user agent from the list."""
    return random.choice(USER_AGENTS)

def get_job_id(url):
    """Generate a unique job ID for a URL."""
    return hashlib.md5(url.encode()).hexdigest()

async def get_session(use_zyte=False):
    """Create an aiohttp session with optional proxy."""
    headers = {"User-Agent": get_random_user_agent()}
    
    if use_zyte:
        # Use Zyte proxy
        proxies = { 'http': f"http://{CONFIG['zyte_api_key']}:@proxy.zyte.com:8011",
        'https': f"http://{CONFIG['zyte_api_key']}:@proxy.zyte.com:8011"}
        connector = ProxyConnector.from_url(ZYTE_PROXY_URL, ssl=True, verify_ssl=False, ssl_ca_path="zyte-ca.crt")
        logger.info("Using Zyte proxy for this request")
        return aiohttp.ClientSession(headers=headers, connector=connector)
    else:
        # Use random proxy if available
        proxy = get_random_proxy()
        if proxy:
            connector = ProxyConnector.from_url(proxy)
            return aiohttp.ClientSession(headers=headers, connector=connector)
        else:
            return aiohttp.ClientSession(headers=headers)

async def record_failed_url(url, error):
    """Record a failed URL to the failed URLs file."""
    async with aio_open(CONFIG["failed_urls_file"], "a") as f:
        await f.write(f"{url}\t{error}\t{datetime.now().isoformat()}\n")

@retry(stop=stop_after_attempt(CONFIG["max_attempts"]), 
       wait=wait_exponential(multiplier=1, min=CONFIG["retry_delay"], max=CONFIG["retry_delay"]*2))
async def fetch_page(session, url, use_zyte=False):
    """Fetch a web page with retries and handling for specific error codes."""
    delay = random.uniform(CONFIG["min_delay"], CONFIG["max_delay"])
    await asyncio.sleep(delay)
    
    ssl_retries = 0
    max_ssl_retries = 3
    ssl_retry_delay = random.uniform(2, 5) * 60  # Random delay between 2-5 minutes

    while ssl_retries < max_ssl_retries:
        try:
            async with session.get(url) as response:
                if response.status in [403, 429, 443]:
                    logger.warning(f"Rate limited ({response.status}) for {url}. Switching to Zyte proxy...")
                    
                    # If we're already using Zyte and still got rate limited, wait and retry
                    if use_zyte:
                        logger.warning(f"Still rate limited with Zyte proxy. Waiting {CONFIG['retry_delay']} seconds...")
                        await asyncio.sleep(CONFIG['retry_delay'])
                    else:
                        # Switch to Zyte proxy for the retry
                        # logger.info(f"creating sesssion")
                        async with await get_session(use_zyte=True) as zyte_session:
                            return await fetch_page(zyte_session, url, use_zyte=True)
                    
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status
                    )
                
                response.raise_for_status()
                return await response.text()
                
        except aiohttp.ClientConnectorError as e:
            if "ssl:default" in str(e) or "Cannot connect to host" in str(e):
                ssl_retries += 1
                if ssl_retries < max_ssl_retries:
                    logger.warning(f"SSL connection failed for {url}. Attempt {ssl_retries}/{max_ssl_retries}. Waiting {ssl_retry_delay} seconds...")
                    await asyncio.sleep(ssl_retry_delay)
                    continue
            
            # If we're not already using Zyte, try with it
            if not use_zyte:
                logger.warning(f"Connection error for {url}. Switching to Zyte proxy...")
                async with await get_session(use_zyte=True) as zyte_session:
                    return await fetch_page(zyte_session, url, use_zyte=True)
            
            logger.error(f"Error fetching {url}: {e}")
            await record_failed_url(url, str(e))
            raise
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            if isinstance(e, aiohttp.ClientResponseError):
                if e.status in [403, 429, 443]:
                    logger.error(f"Rate limiting detected. Status: {e.status}")
                elif e.status >= 500:
                    logger.error(f"Server error. Status: {e.status}")
            
            # If we're not already using Zyte, try with it
            if not use_zyte:
                logger.warning(f"Error for {url}. Switching to Zyte proxy...")
                async with await get_session(use_zyte=True) as zyte_session:
                    return await fetch_page(zyte_session, url, use_zyte=True)
                    
            await record_failed_url(url, str(e))
            raise

async def get_download_links(session, item_url, use_zyte=False):
    """Find download links for the specified format."""
    logger.info(f"Finding download links for {item_url}")
    
    try:
        html = await fetch_page(session, item_url, use_zyte)
        soup = BeautifulSoup(html, 'html.parser')
        
        download_links = {}
        links = soup.find_all('a')
        for link in links:
            if 'download' in link.text:
                text = link.text.strip().replace('download','').strip()
                href = link.get('href', '')
                if 'HOCR' == text or (href and href.endswith('.html')):
                    download_links['HOCR'] = href
                if 'FULL TEXT' == text:
                    download_links['TEXT'] = href
                if 'PDF' == text:
                    download_links['PDF'] = href
                    
        logger.info(f"Found {len(download_links)} download links for {item_url}")
        
        # Create a list of download links with priority order
        result = []
        if 'HOCR' in download_links:
            result.append(download_links['HOCR'])
        if 'TEXT' in download_links and len(result)==0:
            result.append(download_links['TEXT'])
        if 'PDF' in download_links and len(result)==0:
            result.append(download_links['PDF'])
            
        return result
    except Exception as e:
        logger.error(f"Error getting download links for {item_url}: {e}")
        # If we're not already using Zyte, try with it
        if not use_zyte:
            logger.warning(f"Trying to get download links with Zyte proxy for {item_url}")
            async with await get_session(use_zyte=True) as zyte_session:
                return await get_download_links(zyte_session, item_url, True)
        raise

async def download_file(session, url, output_folder, item_name, use_zyte=False):
    """Download a file with resume capability."""
    # Create a subfolder for each item
    item_folder = os.path.join(output_folder, item_name)
    os.makedirs(item_folder, exist_ok=True)
    
    # Clean filename and limit length
    local_filename = os.path.basename(url)[:200]
    if not local_filename:
        local_filename = f"file_{hashlib.md5(url.encode()).hexdigest()}.pdf"
    
    local_path = os.path.join(item_folder, local_filename)
    temp_path = f"{local_path}.part"
    
    # Check if file already exists and is complete
    if os.path.exists(local_path) and CONFIG["resume_downloads"]:
        return local_path
    
    # Get file size if resuming
    headers = {}
    if os.path.exists(temp_path) and CONFIG["resume_downloads"]:
        file_size = os.path.getsize(temp_path)
        headers["Range"] = f"bytes={file_size}-"
    else:
        file_size = 0
    
    try:
        async with session.get(url, headers=headers) as response:
            # Check for rate limiting
            if response.status in [403, 429, 443]:
                if not use_zyte:
                    logger.warning(f"Rate limited when downloading {url}. Switching to Zyte proxy...")
                    async with await get_session(use_zyte=True) as zyte_session:
                        return await download_file(zyte_session, url, output_folder, item_name, True)
                else:
                    logger.warning(f"Still rate limited with Zyte proxy for {url}. Waiting {CONFIG['retry_delay']} seconds...")
                    await asyncio.sleep(CONFIG['retry_delay'])
                    return await download_file(session, url, output_folder, item_name, use_zyte)
            
            response.raise_for_status()
            
            total_size = int(response.headers.get('Content-Length', 0))
            if 'Content-Range' in response.headers:
                total_size = int(response.headers.get('Content-Range').split('/')[-1])
            
            mode = 'ab' if file_size > 0 else 'wb'
            
            with tqdm(total=total_size, initial=file_size, unit='B', unit_scale=True, desc=local_filename) as progress:
                async with aio_open(temp_path, mode) as f:
                    chunk_size = 8192
                    async for chunk in response.content.iter_chunked(chunk_size):
                        await f.write(chunk)
                        progress.update(len(chunk))
            
            # Rename the temp file to the final filename
            os.rename(temp_path, local_path)
            logger.info(f"Downloaded: {local_path}")
            return local_path
            
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        
        # If we're not already using Zyte, try with it
        if not use_zyte:
            logger.warning(f"Trying download with Zyte proxy for {url}")
            async with await get_session(use_zyte=True) as zyte_session:
                return await download_file(zyte_session, url, output_folder, item_name, True)
        
        return None

async def process_item(item_url, semaphore):
    """Process a single item with improved error handling."""
    job_id = get_job_id(item_url)
    retries = 0
    
    while retries < CONFIG["max_attempts"]:
        try:
            async with semaphore:
                # Check if job is already in progress or completed
                metadata_path = os.path.join(CONFIG["metadata_folder"], f"{job_id}_metadata.json")
                if os.path.exists(metadata_path) and CONFIG["resume_downloads"]:
                    logger.info(f"Item {item_url} already processed. Skipping.")
                    return
                
                logger.info(f"Processing {item_url}")
                item_name = item_url.split('/')[-1]
                
                start_time = datetime.utcnow()
                file_paths = []
                
                try:
                    # First try without Zyte
                    use_zyte = False
                    try:
                        async with await get_session(use_zyte=False) as session:
                            download_links = await get_download_links(session, item_url)
                    except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError) as e:
                        # If rate limited or connection error, try with Zyte
                        if isinstance(e, aiohttp.ClientResponseError) and e.status in [403, 429, 443]:
                            logger.warning(f"Rate limited for {item_url}. Switching to Zyte proxy...")
                            use_zyte = True
                        elif isinstance(e, aiohttp.ClientConnectorError):
                            logger.warning(f"Connection error for {item_url}. Switching to Zyte proxy...")
                            use_zyte = True
                        
                        async with await get_session(use_zyte=True) as session:
                            download_links = await get_download_links(session, item_url, True)
                    
                    # Use a separate semaphore for downloads to limit concurrent downloads per item
                    download_sem = asyncio.Semaphore(CONFIG["concurrent_downloads"])
                    download_tasks = []
                    
                    # Use the appropriate session based on whether we're using Zyte
                    async with await get_session(use_zyte=use_zyte) as session:
                        for link in download_links:
                            full_url = urljoin(CONFIG["base_url"], link) if not link.startswith('http') else link
                            task = asyncio.create_task(
                                download_with_semaphore(session, full_url, CONFIG["output_folder"], item_name, download_sem, use_zyte)
                            )
                            download_tasks.append(task)
                        
                        file_paths = await asyncio.gather(*download_tasks)
                        file_paths = [p for p in file_paths if p]
                    
                    end_time = datetime.utcnow()
                    
                    # Calculate folder size
                    item_folder = os.path.join(CONFIG["output_folder"], item_name)
                    folder_size = sum(os.path.getsize(f) for f in file_paths if f and os.path.exists(f))
                    
                    metadata = {
                        'folder_name': item_name,
                        'url': item_url,
                        'start_time': start_time.isoformat(),
                        'end_time': end_time.isoformat(),
                        'duration_seconds': (end_time - start_time).total_seconds(),
                        'folder_size': folder_size,
                        'download_links': download_links,
                        'total_files': len(file_paths),
                        'success_count': len([p for p in file_paths if p]),
                        'failed_links': [link for i, link in enumerate(download_links) if i < len(file_paths) and not file_paths[i]]
                    }
                    
                    # Write metadata to a local file
                    async with aio_open(metadata_path, 'w') as f:
                        await f.write(json.dumps(metadata, indent=2))
                        
                    logger.info(f"Item {item_url} processed. Downloaded {len(file_paths)} files.")
                        
                except Exception as e:
                    logger.error(f"Error processing item {item_url}: {e}")
                return  # Success
        except Exception as e:
            retries += 1
            logger.error(f"Attempt {retries}/{CONFIG['max_attempts']} failed for {item_url}: {e}")
            if retries < CONFIG["max_attempts"]:
                await asyncio.sleep(CONFIG["retry_delay"])
            else:
                logger.error(f"All attempts failed for {item_url}")
                async with aio_open(CONFIG["failed_urls_file"], "a") as f:
                    await f.write(f"{item_url}\tMax retries exceeded: {str(e)}\n")
                return

async def download_with_semaphore(session, url, output_folder, item_name, semaphore, use_zyte=False):
    """Download a file with a semaphore to limit concurrent downloads."""
    async with semaphore:
        return await download_file(session, url, output_folder, item_name, use_zyte)

async def process_batch(item_urls):
    """Process a batch of URLs concurrently."""
    logger.info(f"Processing batch of {len(item_urls)} items")
    semaphore = asyncio.Semaphore(CONFIG["concurrent_items"])
    tasks = [process_item(url, semaphore) for url in item_urls]
    await asyncio.gather(*tasks)
    logger.info(f"Batch of {len(item_urls)} items completed")

async def main_async(item_urls):
    """Main async function to process all items in batches."""
    logger.info(f"Starting to process {len(item_urls)} items")
    
    # Process in batches to avoid exhausting resources
    for i in range(0, len(item_urls), CONFIG["batch_size"]):
        batch = item_urls[i:i+CONFIG["batch_size"]]
        logger.info(f"Processing batch {i//CONFIG['batch_size']+1}/{(len(item_urls)-1)//CONFIG['batch_size']+1}")
        await process_batch(batch)
        
        # Add some delay between batches to avoid overwhelming the server
        await asyncio.sleep(random.uniform(5, 15))
    
    logger.info("All items processed")

def load_urls_from_file(filename):
    """Load URLs from a file, one per line."""
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]

def main():
    # Sample URLs - replace with your actual source
    item_urls = [
        "https://archive.org/details/chandamamasanskrit"
    ]
    
    # Load from file if specified
    if os.path.exists("archive_urls_urdu.txt"): # pass the path of urls
        item_urls = load_urls_from_file("archive_urls_urdu.txt")
        logger.info(f"Loaded {len(item_urls)} URLs from archive_urls_urdu.txt")
    
    # Run the async main function
    asyncio.run(main_async(item_urls))

if __name__ == "__main__":
    main()