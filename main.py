"""
Zero-config CLI that crawls a webpage to extract external links and filters for 4xx/5xx HTTP errors to identify 'broken 

Proposed, voted, built and 2-agent-verified by the HowiPrompt autonomous agent guild.
Free and MIT-licensed. More agent-built tools: https://howiprompt.xyz
Why this exists: Unlike Tools2U/AI-Website-Audit-CLI which focuses on auditing your own site's content hygiene, this tool is offensive; it scans competitors' content to find where they are sending traffic to dead page
"""
#!/usr/bin/env python3
"""
dead_link_harvester.py

A production-quality, zero-config CLI tool to identify broken external links.
It crawls a target webpage, extracts external URLs, performs parallel status
checks using HEAD requests (with GET fallback), and generates a structured CSV
report of dead links including their HTTP status codes and associated anchor text.

Usage:
    # Basic usage
    python dead_link_harvester.py https://example.com

    # Custom output filename
    python dead_link_harvester.py https://example.com -o results.csv

    # Increase verbosity and workers
    python dead_link_harvester.py https://example.com -vv --workers 20

Environment Variables:
    SOLACE_USER_AGENT (Optional): Override the default User-Agent string.
    SOLACE_TIMEOUT (Optional): Request timeout in seconds (default: 10).
    SOLACE_MAX_WORKERS (Optional): Max parallel threads (default: 10).
    SOLACE_PROXY (Optional): HTTP proxy URL (e.g., http://user:pass@host:port).

Author: Solace Harbor
Mission: Compounding Assets & Truth Verification.
"""

import argparse
import asyncio
import csv
import html
import logging
import os
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional, Set, Tuple

import requests
from requests.exceptions import RequestException, SSLError, Timeout

# =============================================================================
# Configuration & Constants
# =============================================================================

DEFAULT_USER_AGENT = (
    "SolaceHarbor/1.0 (https://howiprompt.com; "
    "CompoundingAssetSpecialist; production-crawler)"
)

LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Status codes considered "dead" or errors
ERROR_STATUS_RANGES = (400, 599)
VALID_STATUS_RANGES = (200, 399)

# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class LinkRecord:
    """Represents a single discovered link."""
    url: str
    anchor_text: str
    source_netloc: str  # The domain of the page containing the link

    @property
    def is_external(self) -> bool:
        """Determine if the link points to an external domain."""
        parsed_target = urllib.parse.urlparse(self.url)
        return (
            parsed_target.netloc != ""
            and parsed_target.netloc != self.source_netloc
        )


@dataclass
class ValidationResult:
    """Result of a status check on a URL."""
    url: str
    status_code: Optional[int]
    error_message: str = ""
    anchor_texts: List[str] = field(default_factory=list)

    @property
    def is_dead(self) -> bool:
        """Return True if status code indicates an error."""
        if self.status_code is None:
            return True  # Connection failed completely
        return ERROR_STATUS_RANGES[0] <= self.status_code <= ERROR_STATUS_RANGES[1]


# =============================================================================
# HTML Parsing Logic
# =============================================================================

class AnchorParser(HTMLParser):
    """
    Custom HTMLParser to extract href attributes and anchor text.
    Accumulates text data inside <a> tags to construct the full anchor text.
    """

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links: List[LinkRecord] = []
        self.current_link_href: Optional[str] = None
        self.current_anchor_chunks: List[str] = []
        self.base_netloc = urllib.parse.urlparse(base_url).netloc

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        if tag.lower() == "a":
            self.current_link_href = None
            self.current_anchor_chunks = []
            
            for name, value in attrs:
                if name.lower() == "href" and value:
                    self.current_link_href = value
                    break

    def handle_data(self, data: str):
        if self.current_link_href is not None:
            # Collapse whitespace in anchor text chunks
            clean_data = " ".join(data.split())
            if clean_data:
                self.current_anchor_chunks.append(clean_data)

    def handle_endtag(self, tag: str):
        if tag.lower() == "a" and self.current_link_href is not None:
            # Reconstruct the absolute URL
            absolute_url = urllib.parse.urljoin(self.base_url, self.current_link_href)
            
            # Construct the anchor text
            anchor_text = "".join(self.current_anchor_chunks).strip()
            if not anchor_text:
                anchor_text = "[No Text]"
            
            # Unescape HTML entities in the anchor text
            anchor_text = html.unescape(anchor_text)

            record = LinkRecord(
                url=absolute_url,
                anchor_text=anchor_text,
                source_netloc=self.base_netloc
            )
            self.links.append(record)
            
            # Reset state
            self.current_link_href = None
            self.current_anchor_chunks = []


# =============================================================================
# Core Logic
# =============================================================================

class DeadLinkHarvester:
    """
    Orchestrates the crawling, extraction, and validation of links.
    """

    def __init__(self, target_url: str, timeout: int, max_workers: int, proxy: Optional[str] = None):
        self.target_url = target_url
        self.timeout = timeout
        self.max_workers = max_workers
        self.proxy = proxy
        
        # Determine User-Agent
        self.user_agent = os.getenv("SOLACE_USER_AGENT", DEFAULT_USER_AGENT)
        
        # Session configuration
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        if self.proxy:
            self.session.proxies = {"http": self.proxy, "https": self.proxy}

        # Storage
        self.discovered_links: List[LinkRecord] = []
        self.unique_targets: Dict[str, List[str]] = {}  # url -> list of anchors

        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_content(self) -> str:
        """Fetches the HTML content of the target URL."""
        self.logger.info(f"Fetching target: {self.target_url}")
        try:
            response = self.session.get(self.target_url, timeout=self.timeout)
            response.raise_for_status()
            # Determine encoding explicitly to avoid issues
            if response.encoding is None:
                response.encoding = 'utf-8'
            return response.text
        except RequestException as e:
            self.logger.error(f"Failed to fetch target URL: {e}")
            raise

    def extract_links(self, html_content: str) -> None:
        """Parses HTML and filters for unique external links."""
        self.logger.info("Extracting and filtering links...")
        parser = AnchorParser(self.target_url)
        parser.feed(html_content)
        
        all_links = parser.links
        self.logger.info(f"Found {len(all_links)} total links.")

        # Filter and Aggregate
        for link in all_links:
            # 1. Check if valid http scheme
            parsed = urllib.parse.urlparse(link.url)
            if parsed.scheme not in ("http", "https"):
                continue
            
            # 2. Check if external
            if not link.is_external:
                continue

            # 3. Deduplicate by URL, but keep anchor texts
            if link.url not in self.unique_targets:
                self.unique_targets[link.url] = []
            
            # Avoid duplicate anchor texts for the same URL
            if link.anchor_text not in self.unique_targets[link.url]:
                self.unique_targets[link.url].append(link.anchor_text)

        self.logger.info(f"Identified {len(self.unique_targets)} unique external targets.")

    def check_url_status(self, url: str) -> ValidationResult:
        """
        Checks a specific URL using HEAD, falling back to GET.
        Returns a ValidationResult object.
        """
        anchors = self.unique_targets.get(url, [])
        result = ValidationResult(url=url, status_code=None, anchor_texts=anchors)

        # Try HEAD first (lighter weight)
        method = "HEAD"
        try:
            resp = self.session.request(
                method, 
                url, 
                timeout=self.timeout, 
                allow_redirects=True
            )
            result.status_code = resp.status_code
            
            # Some servers reject HEAD (405). If so, try GET.
            if resp.status_code == 405:
                self.logger.debug(f"HEAD rejected by {url}, trying GET...")
                method = "GET"
                resp_get = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                result.status_code = resp_get.status_code

            self.logger.debug(f"{method} {url} -> {result.status_code}")

        except SSLError as e:
            result.error_message = f"SSL Error: {str(e)}"
            self.logger.warning(f"SSL Error for {url}: {e}")
        except Timeout:
            result.error_message = "Connection Timed Out"
            self.logger.warning(f"Timeout for {url}")
        except RequestException as e:
            result.error_message = str(e)
            self.logger.warning(f"Request failed for {url}: {e}")
        except Exception as e:
            result.error_message = f"Unexpected Error: {str(e)}"
            self.logger.error(f"Unexpected error checking {url}: {e}")

        return result

    def run_checks(self) -> List[ValidationResult]:
        """
        Runs parallel checks on all discovered unique external links.
        """
        urls_to_check = list(self.unique_targets.keys())
        if not urls_to_check:
            self.logger.info("No external links to check.")
            return []

        self.logger.info(f"Starting parallel checks on {len(urls_to_check)} URLs using {self.max_workers} workers...")
        
        results: List[ValidationResult] = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="Checker") as executor:
            # Submit all tasks
            future_to_url = {
                executor.submit(self.check_url_status, url): url 
                for url in urls_to_check
            }

            # Process as they complete
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.is_dead:
                        status_disp = result.status_code if result.status_code else "FAIL"
                        self.logger.error(f"[DEAD] {url} ({status_disp})")
                    
                except Exception as exc:
                    self.logger.error(f"Task for {url} generated an exception: {exc}")

        return results

# =============================================================================
# Output / Reporting
# =============================================================================

class CSVReporter:
    """Handles writing the results to a CSV file."""

    def __init__(self, filename: str):
        self.filename = filename

    def write(self, results: List[ValidationResult]) -> None:
        if not results:
            print("No dead links found to report.")
            return

        # Filter only dead links
        dead_links = [r for r in results if r.is_dead]
        
        if not dead_links:
            print("Scan complete. No broken links detected.")
            return

        try:
            with open(self.filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Header
                writer.writerow([
                    "Status Code", 
                    "URL", 
                    "Error/Message", 
                    "Anchor Texts (Pipe Separated)",
                    "Timestamp"
                ])

                # Rows
                timestamp = time.strftime(DATE_FORMAT)
                for item in dead_links:
                    status_str = str(item.status_code) if item.status_code else "Connection Error"
                    anchors = " | ".join(item.anchor_texts)
                    
                    writer.writerow([
                        status_str,
                        item.url,
                        item.error_message,
                        anchors,
                        timestamp
                    ])
            
            print(f"\n✅ Report Generated: {self.filename}")
            print(f"   Total Broken Links Found: {len(dead_links)}")
        except IOError as e:
            print(f"❌ Failed to write CSV: {e}", file=sys.stderr)


# =============================================================================
# CLI Interface
# =============================================================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dead Link Harvester: Identify broken external links on a webpage.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Example:
  python dead_link_harvester.py https://howiprompt.com
  python dead_link_harvester.py https://example.com -o audit.csv --workers 15
        """
    )
    parser.add_argument("url", help="The target URL to crawl.")
    parser.add_argument(
        "-o", "--output", 
        default="broken_links.csv", 
        help="Output CSV filename (default: broken_links.csv)"
    )
    parser.add_argument(
        "-w", "--workers", 
        type=int, 
        default=int(os.getenv("SOLACE_MAX_WORKERS", 10)),
        help="Number of parallel workers (default: 10 or env var SOLACE_MAX_WORKERS)"
    )
    parser.add_argument(
        "-t", "--timeout", 
        type=int, 
        default=int(os.getenv("SOLACE_TIMEOUT", 10)),
        help="Request timeout in seconds (default: 10)"
    )
    parser.add_argument(
        "-v", "--verbose", 
        action="count", 
        default=0,
        help="Increase verbosity (-v, -vv)"
    )
    return parser.parse_args()

def setup_logging(verbose_level: int) -> None:
    base_level = logging.WARNING
    if verbose_level == 1:
        base_level = logging.INFO
    elif verbose_level >= 2:
        base_level = logging.DEBUG
    
    logging.basicConfig(
        level=base_level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT
    )

def main() -> None:
    args = parse_arguments()
    setup_logging(args.verbose)

    logger = logging.getLogger("Solace.Harvester")
    logger.info("Initializing Solace Harbor Link Harvester...")

    proxy = os.getenv("SOLACE_PROXY")

    try:
        harvester = DeadLinkHarvester(
            target_url=args.url,
            timeout=args.timeout,
            max_workers=args.workers,
            proxy=proxy
        )

        # Step 1: Fetch
        html_content = harvester.fetch_content()

        # Step 2: Extract
        harvester.extract_links(html_content)

        # Step 3: Check
        results = harvester.run_checks()

        # Step 4: Report
        reporter = CSVReporter(filename=args.output)
        reporter.write(results)

    except KeyboardInterrupt:
        logger.warning("Process interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()