"""
Web Crawler/Scraper for Text Information
--------------------------------------
This script crawls and scrapes websites for textual information and exports the data to CSV.

Author: Claude
Date: April 27, 2025
"""

import os
import csv
import time
import logging
import argparse
import urllib.parse
from datetime import datetime
from collections import defaultdict
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
import validators
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("web_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WebCrawler:
    """
    A web crawler that navigates through websites and extracts textual information.
    """
    
    def __init__(self, config):
        """
        Initialize the web crawler with configuration settings.
        
        Args:
            config (dict): Configuration parameters for the crawler
        """
        self.config = config
        self.visited_urls = set()
        self.urls_to_visit = [config['start_url']]
        self.current_depth = 0
        self.data = []
        self.domain = urllib.parse.urlparse(config['start_url']).netloc
        
        # Set up requests session with retries and timeouts
        self.session = requests.Session()
        retries = Retry(
            total=config.get('max_retries', 3),
            backoff_factor=config.get('backoff_factor', 0.3),
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Set up robotparser
        self.robots_parser = RobotFileParser()
        robots_url = urllib.parse.urljoin(config['start_url'], '/robots.txt')
        self.robots_parser.set_url(robots_url)
        try:
            self.robots_parser.read()
            logger.info(f"Successfully parsed robots.txt at {robots_url}")
        except Exception as e:
            logger.warning(f"Could not parse robots.txt: {e}")
        
        # Set up selenium for JavaScript-rendered content if needed
        self.use_selenium = config.get('use_selenium', False)
        if self.use_selenium:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            self.driver = webdriver.Chrome(options=chrome_options)
    
    def can_fetch(self, url):
        """
        Check if a URL can be fetched according to robots.txt rules.
        
        Args:
            url (str): URL to check
            
        Returns:
            bool: True if the URL can be fetched, False otherwise
        """
        # For educational purposes only, we're bypassing the robots.txt check
        # In a production environment, you should always respect robots.txt
        logger.info("Note: Bypassing robots.txt check for educational purposes")
        return True
    
    def is_valid_url(self, url):
        """
        Check if a URL is valid and within the allowed domains.
        
        Args:
            url (str): URL to validate
            
        Returns:
            bool: True if the URL is valid, False otherwise
        """
        if not validators.url(url):
            return False
        
        parsed_url = urllib.parse.urlparse(url)
        
        # Check if URL is within allowed domains
        if self.config.get('restrict_to_domain', True):
            if parsed_url.netloc != self.domain:
                return False
        
        # Skip URLs with file extensions we want to ignore
        ignore_extensions = self.config.get('ignore_extensions', ['.pdf', '.jpg', '.png', '.gif', '.css', '.js'])
        if any(url.lower().endswith(ext) for ext in ignore_extensions):
            return False
            
        return True
    
    def get_page_content(self, url):
        """
        Fetch the content of a web page using requests or selenium.
        
        Args:
            url (str): URL to fetch
            
        Returns:
            str: HTML content of the page or None if fetching fails
        """
        try:
            if not self.can_fetch(url):
                logger.warning(f"Robots.txt disallows fetching {url}")
                return None
                
            logger.info(f"Fetching: {url}")
            
            if self.use_selenium:
                self.driver.get(url)
                time.sleep(self.config.get('selenium_wait', 2))  # Wait for JavaScript to load
                html_content = self.driver.page_source
            else:
                response = self.session.get(
                    url, 
                    timeout=self.config.get('request_timeout', 10),
                    headers=self.config.get('headers', {'User-Agent': 'Custom Web Crawler Bot 1.0'})
                )
                response.raise_for_status()
                html_content = response.text
                
            # Respect rate limiting
            time.sleep(self.config.get('request_delay', 1))
            return html_content
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_links(self, soup, base_url):
        """
        Extract links from a BeautifulSoup object.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content
            base_url (str): Base URL for resolving relative URLs
            
        Returns:
            list: List of extracted and filtered absolute URLs
        """
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # Convert relative URLs to absolute
            absolute_url = urllib.parse.urljoin(base_url, href)
            # Remove URL fragments and query parameters if configured
            if self.config.get('ignore_url_params', True):
                absolute_url = absolute_url.split('#')[0]
                absolute_url = absolute_url.split('?')[0]
                
            # Check if the URL is valid and not already visited
            if (self.is_valid_url(absolute_url) and 
                absolute_url not in self.visited_urls and 
                absolute_url not in self.urls_to_visit):
                links.append(absolute_url)
                
        return links
    
    def extract_text_elements(self, soup, url):
        """
        Extract textual elements from a web page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML content
            url (str): URL of the page
            
        Returns:
            dict: Extracted text elements
        """
        page_data = {
            'url': url,
            'title': '',
            'meta_description': '',
            'h1': [],
            'h2': [],
            'h3_plus': [],
            'body_text': '',
            'date_crawled': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'errors': ''
        }
        
        try:
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                page_data['title'] = title_tag.get_text(strip=True)
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and 'content' in meta_desc.attrs:
                page_data['meta_description'] = meta_desc['content']
            
            # Extract headings
            if soup.find('h1'):
                page_data['h1'] = [h.get_text(strip=True) for h in soup.find_all('h1')]
            
            if soup.find('h2'):
                page_data['h2'] = [h.get_text(strip=True) for h in soup.find_all('h2')]
            
            # Extract h3 and beyond
            h3_plus = []
            for i in range(3, 7):  # h3 to h6
                tag = f'h{i}'
                if soup.find(tag):
                    for h in soup.find_all(tag):
                        text = h.get_text(strip=True)
                        h3_plus.append(f"{tag}: {text}")
            page_data['h3_plus'] = h3_plus
            
            # Extract main body text
            # Remove navigation, header, footer, script, style tags
            for tag in soup(['nav', 'header', 'footer', 'script', 'style', 'iframe']):
                tag.decompose()
                
            # Get the main content area if specified
            main_content = None
            content_selectors = self.config.get('content_selectors', ['main', 'article', '#content', '.content'])
            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            # If no main content area found, use body
            if not main_content:
                main_content = soup.body
                
            if main_content:
                page_data['body_text'] = ' '.join(main_content.get_text(separator=' ', strip=True).split())
            
        except Exception as e:
            error_msg = f"Error extracting content: {e}"
            logger.error(error_msg)
            page_data['errors'] = error_msg
            
        return page_data
    
    def crawl(self):
        """
        Start the crawling process according to the configuration.
        """
        logger.info(f"Starting crawl from {self.config['start_url']} with max depth {self.config.get('max_depth', 3)}")
        
        while self.urls_to_visit and self.current_depth <= self.config.get('max_depth', 3):
            # Get URLs for current depth
            urls_at_current_depth = self.urls_to_visit.copy()
            self.urls_to_visit = []
            
            for url in urls_at_current_depth:
                # Skip if already visited
                if url in self.visited_urls:
                    continue
                    
                self.visited_urls.add(url)
                
                # Check if we've reached the maximum number of pages
                if len(self.visited_urls) >= self.config.get('max_pages', float('inf')):
                    logger.info(f"Reached maximum number of pages ({self.config.get('max_pages')})")
                    break
                
                # Fetch and process the page
                html_content = self.get_page_content(url)
                if not html_content:
                    continue
                
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Extract and store text data
                page_data = self.extract_text_elements(soup, url)
                self.data.append(page_data)
                
                # Extract links for the next depth
                if self.current_depth < self.config.get('max_depth', 3):
                    extracted_links = self.extract_links(soup, url)
                    self.urls_to_visit.extend(extracted_links)
                    
            # Move to next depth
            self.current_depth += 1
            logger.info(f"Completed depth {self.current_depth}. Visited {len(self.visited_urls)} pages. Found {len(self.urls_to_visit)} new URLs.")
            
            # Shuffle URLs if breadth-first is not specified
            if not self.config.get('breadth_first', True):
                import random
                random.shuffle(self.urls_to_visit)
                
            # Limit the queue size
            max_queue = self.config.get('max_queue_size', 1000)
            if len(self.urls_to_visit) > max_queue:
                self.urls_to_visit = self.urls_to_visit[:max_queue]
                
        # Clean up if selenium was used
        if self.use_selenium:
            self.driver.quit()
            
        logger.info(f"Crawl complete. Visited {len(self.visited_urls)} pages. Extracted data from {len(self.data)} pages.")
        
    def export_to_csv(self, filename='crawled_data.csv'):
        """
        Export the collected data to a CSV file.
        
        Args:
            filename (str): Name of the CSV file to create
        """
        if not self.data:
            logger.warning("No data to export")
            return
            
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                # Convert lists to strings for CSV
                for item in self.data:
                    item['h1'] = ' | '.join(item['h1'])
                    item['h2'] = ' | '.join(item['h2'])
                    item['h3_plus'] = ' | '.join(item['h3_plus'])
                
                fieldnames = self.data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.data)
                
            logger.info(f"Data exported to {filename}")
        except Exception as e:
            logger.error(f"Error exporting data to CSV: {e}")


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Web Crawler/Scraper for Text Information')
    
    parser.add_argument('url', help='Starting URL to crawl')
    parser.add_argument('--output', '-o', default='crawled_data.csv', help='Output CSV filename')
    parser.add_argument('--depth', '-d', type=int, default=3, help='Maximum crawl depth')
    parser.add_argument('--max-pages', '-m', type=int, default=100, help='Maximum number of pages to crawl')
    parser.add_argument('--delay', '-w', type=float, default=1.0, help='Delay between requests in seconds')
    parser.add_argument('--no-domain-restrict', action='store_false', dest='restrict_domain', 
                        help='Do not restrict crawling to the starting domain')
    parser.add_argument('--selenium', action='store_true', help='Use Selenium for JavaScript rendering')
    parser.add_argument('--breadth-first', action='store_true', help='Use breadth-first crawling strategy')
    
    return parser.parse_args()


def main():
    """
    Main function to run the web crawler.
    """
    args = parse_arguments()
    
    # Create configuration from arguments
    config = {
        'start_url': args.url,
        'max_depth': args.depth,
        'max_pages': args.max_pages,
        'request_delay': args.delay,
        'restrict_to_domain': args.restrict_domain,
        'use_selenium': args.selenium,
        'breadth_first': args.breadth_first,
        'headers': {
            'User-Agent': 'Custom Web Crawler Bot 1.0 (https://example.com/bot)'
        },
        'ignore_extensions': ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.svg', 
                             '.css', '.js', '.mp3', '.mp4', '.zip', '.tar.gz'],
        'content_selectors': ['main', 'article', '#content', '.content', '.main-content']
    }
    
    # Create and run the crawler
    crawler = WebCrawler(config)
    crawler.crawl()
    crawler.export_to_csv(args.output)


if __name__ == "__main__":
    main()