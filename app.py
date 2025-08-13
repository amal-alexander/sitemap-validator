import asyncio
import datetime
import io
import json
import re
from collections import deque, defaultdict
from urllib.parse import urljoin, urlparse, urldefrag

import aiohttp
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

APP_TITLE = "Enhanced Sitemap Generator + Checker (Up to 1 Lakh URLs)"

# -------------------------
# Configuration & Constants
# -------------------------
MAX_URLS = 500000  # 5 lakh URLs (increased capacity)
DEFAULT_CONCURRENCY = 100  # Much higher for speed
CHUNK_SIZE = 2000  # Larger chunks for better throughput
BATCH_SIZE = 200  # Process more URLs per batch

EXTENSION_BLACKLIST = re.compile(
    r"\.(?:png|jpg|jpeg|webp|gif|svg|ico|css|js|mjs|woff2?|ttf|eot|zip|rar|7z|gz|tar|dmg|exe|apk|pdf|docx?|xlsx?|pptx?)$",
    re.I,
)

# -------------------------
# Utility helpers (optimized)
# -------------------------
def strip_fragment(url: str) -> str:
    if not url:
        return url
    clean, _ = urldefrag(url)
    return clean

def normalize_url(url: str, preserve_trailing_slash: bool = False) -> str:
    """
    Normalize URL with option to preserve trailing slashes
    
    Args:
        url: URL to normalize
        preserve_trailing_slash: If True, preserve trailing slashes as they appear in original URL
    """
    if not url:
        return url
    url = strip_fragment(url)
    p = urlparse(url)
    scheme = p.scheme.lower() or "http"
    netloc = p.netloc.lower()
    path = p.path or "/"
    
    if not path.startswith("/"):
        path = "/" + path
    
    # Remove default ports
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]
    
    # Handle trailing slashes intelligently
    if not preserve_trailing_slash:
        # Only remove trailing slash if it's not the root path
        if path != "/" and path.endswith("/"):
            path = path[:-1]
    # If preserve_trailing_slash=True, keep the path as-is
    
    rebuilt = f"{scheme}://{netloc}{path}"
    if p.query:
        rebuilt += f"?{p.query}"
    return rebuilt

def same_site(url_a: str, url_b: str) -> bool:
    pa, pb = urlparse(url_a), urlparse(url_b)
    return pa.netloc.lower() == pb.netloc.lower()

def is_internal_link(link: str, root_url: str) -> bool:
    if not link or link.startswith(("mailto:", "tel:", "javascript:")):
        return False
    abs_url = urljoin(root_url, link)
    return same_site(abs_url, root_url)

def is_crawlable(url: str) -> bool:
    return not EXTENSION_BLACKLIST.search(url or "")

def safe_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def today_iso() -> str:
    return datetime.date.today().isoformat()

# -------------------------
# Enhanced fetching with better error handling
# -------------------------
async def fetch_with_semaphore(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, 
                               url: str, method: str = "GET", timeout: int = 10):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Enhanced-Sitemapper/2.0; +https://example.com/bot)"
    }
    async with semaphore:
        try:
            # First request without following redirects to get original status
            if method == "HEAD":
                async with session.head(url, headers=headers, timeout=timeout, allow_redirects=False) as resp:
                    original_status = resp.status
                    original_headers = dict(resp.headers)
                    
                    # If it's a redirect, get the final URL by following redirects
                    if original_status in [301, 302, 303, 307, 308]:
                        try:
                            async with session.head(url, headers=headers, timeout=timeout, allow_redirects=True) as final_resp:
                                return {
                                    "url": url, "status": original_status, "headers": original_headers,
                                    "text": "", "final_url": str(final_resp.url), 
                                    "content_type": original_headers.get("Content-Type", ""),
                                    "redirect_url": str(final_resp.url),
                                    "final_status": final_resp.status
                                }
                        except:
                            # If following redirects fails, return original response
                            pass
                    
                    return {
                        "url": url, "status": original_status, "headers": original_headers,
                        "text": "", "final_url": str(resp.url), 
                        "content_type": original_headers.get("Content-Type", "")
                    }
            else:
                # For GET requests, check for redirects first
                async with session.get(url, headers=headers, timeout=timeout, allow_redirects=False) as resp:
                    original_status = resp.status
                    original_headers = dict(resp.headers)
                    
                    # If it's a redirect, follow it but preserve original status
                    if original_status in [301, 302, 303, 307, 308]:
                        try:
                            async with session.get(url, headers=headers, timeout=timeout, allow_redirects=True) as final_resp:
                                # Only read full text for HTML content from final response
                                text = ""
                                if "text/html" in final_resp.headers.get("Content-Type", "").lower():
                                    text = await final_resp.text(errors="ignore")
                                
                                return {
                                    "url": url, "status": original_status, "headers": original_headers,
                                    "text": text, "final_url": str(final_resp.url),
                                    "content_type": final_resp.headers.get("Content-Type", ""),
                                    "redirect_url": str(final_resp.url),
                                    "final_status": final_resp.status
                                }
                        except:
                            # If following redirects fails, return original response
                            pass
                    else:
                        # No redirect, read content if HTML
                        text = ""
                        if "text/html" in resp.headers.get("Content-Type", "").lower():
                            text = await resp.text(errors="ignore")
                    
                    return {
                        "url": url, "status": original_status, "headers": original_headers,
                        "text": text, "final_url": str(resp.url),
                        "content_type": original_headers.get("Content-Type", "")
                    }
        except Exception as e:
            return {
                "url": url, "status": None, "headers": {}, "text": "", 
                "final_url": url, "content_type": "", "error": str(e)
            }

# -------------------------
# Parse HTML (optimized)
# -------------------------
def parse_page(root_url: str, url: str, html: str):
    if not html:
        return "", "", "", set()
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Title
    title = safe_text(soup.title.string) if soup.title and soup.title.string else ""
    
    # Meta description
    md_tag = soup.find("meta", attrs={"name": "description"})
    meta_desc = safe_text(md_tag.get("content")) if md_tag and md_tag.get("content") else ""
    
    # Canonical
    can_tag = soup.find("link", rel=lambda x: x and "canonical" in str(x).lower())
    canonical = normalize_url(urljoin(url, can_tag.get("href")), preserve_trailing_slash=True) if can_tag and can_tag.get("href") else ""
    
    # Internal links
    links = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if is_internal_link(href, root_url):
            abs_url = normalize_url(urljoin(url, href), preserve_trailing_slash=True)
            if is_crawlable(abs_url):
                links.add(abs_url)
    
    return title, meta_desc, canonical, links

# -------------------------
# Enhanced crawling with chunked processing
# -------------------------
async def crawl_site_enhanced(seed: str, max_pages: int = MAX_URLS, max_depth: int = 5, 
                             concurrency: int = DEFAULT_CONCURRENCY, progress_cb=None):
    root = normalize_url(seed)
    q = deque([(root, 0)])
    seen = {root}
    results = []
    
    # Ultra-fast connection settings
    conn = aiohttp.TCPConnector(
        limit=concurrency * 3,  # Higher connection pool
        limit_per_host=concurrency * 2,  # More per-host connections
        ssl=False,
        ttl_dns_cache=3600,  # Longer DNS cache
        use_dns_cache=True,
        enable_cleanup_closed=True,
        keepalive_timeout=30
    )
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_read=20)  # Faster timeouts
    semaphore = asyncio.Semaphore(concurrency)
    
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        processed = 0
        
        while q and len(results) < max_pages:
            # Larger batches for speed
            batch_size = min(BATCH_SIZE, len(q), max_pages - len(results))
            batch = [q.popleft() for _ in range(batch_size)]
            
            # Fetch batch with gather for max parallelism
            tasks = [fetch_with_semaphore(session, semaphore, url) for url, _ in batch]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Quick processing of responses
            new_links = set()
            for (url, depth), resp in zip(batch, responses):
                if isinstance(resp, Exception):
                    continue
                    
                # Use original URL for consistency, but get final URL for link extraction
                final_url = resp.get("final_url", url)
                status = resp.get("status")
                text = resp.get("text", "")
                headers = resp.get("headers", {})
                
                # Quick lastmod parsing
                lastmod = ""
                if lm := headers.get("Last-Modified"):
                    try:
                        lastmod = pd.to_datetime(lm, errors="coerce", utc=True).date().isoformat()
                    except:
                        pass
                
                # Fast page parsing for HTML only - use final URL for content parsing
                title = meta_desc = canonical = ""
                out_links = set()
                
                if status and 200 <= status < 300 and "text/html" in resp.get("content_type", "").lower():
                    if text:  # Only parse if we have content
                        title, meta_desc, canonical, out_links = parse_page(root, final_url, text)
                elif resp.get("final_status") and 200 <= resp.get("final_status") < 300 and text:
                    # Handle redirected content
                    title, meta_desc, canonical, out_links = parse_page(root, final_url, text)
                
                # Batch new links for next iteration
                if depth < max_depth:
                    for link in out_links:
                        if link not in seen and same_site(link, root) and len(seen) < max_pages:
                            new_links.add((link, depth + 1))
                
                results.append({
                    "url": url, "status": status, "depth": depth,
                    "title": title, "meta_description": meta_desc, "canonical": canonical,
                    "last_modified": lastmod, "content_type": resp.get("content_type", ""),
                    "error": resp.get("error", ""),
                    "redirect_url": resp.get("redirect_url", ""),
                    "final_status": resp.get("final_status", status)
                })
                
                processed += 1
            
            # Bulk add new links
            for link, depth in new_links:
                if link not in seen:
                    seen.add(link)
                    q.append((link, depth))
            
            if progress_cb:
                progress_cb(processed, min(max_pages, len(seen)))
    
    return pd.DataFrame(results).drop_duplicates(subset=["url"]).reset_index(drop=True)

# -------------------------
# Enhanced URL validation
# -------------------------
async def validate_urls_enhanced(urls, concurrency: int = DEFAULT_CONCURRENCY, progress_cb=None):
    if len(urls) > MAX_URLS:
        st.warning(f"Too many URLs! Limiting to first {MAX_URLS:,} URLs.")
        urls = urls[:MAX_URLS]
    
    # Preserve original URLs but create normalized versions for processing
    original_urls = [url for url in urls if url]
    normalized_urls = [normalize_url(url, preserve_trailing_slash=True) for url in original_urls]
    root = normalized_urls[0] if normalized_urls else ""
    
    conn = aiohttp.TCPConnector(
        limit=concurrency * 2, 
        limit_per_host=concurrency,
        ssl=False,
        ttl_dns_cache=300
    )
    timeout = aiohttp.ClientTimeout(total=None, connect=20, sock_read=30)
    semaphore = asyncio.Semaphore(concurrency)
    
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        # Process in chunks for memory efficiency
        rows = []
        total = len(normalized_urls)
        
        for i in range(0, len(normalized_urls), CHUNK_SIZE):
            chunk = normalized_urls[i:i + CHUNK_SIZE]
            original_chunk = original_urls[i:i + CHUNK_SIZE]
            tasks = [fetch_with_semaphore(session, semaphore, u) for u in chunk]
            
            for j, resp in enumerate(await asyncio.gather(*tasks)):
                # Use original URL from sitemap/input, not normalized version
                original_url = original_chunk[j]
                normalized_url = chunk[j]
                final_url = resp.get("final_url", resp.get("url", normalized_url))
                status = resp.get("status")
                text = resp.get("text", "")
                headers = resp.get("headers", {})
                
                # Check for trailing slash redirects
                redirect_info = ""
                if resp.get("redirect_url") and resp.get("redirect_url") != normalized_url:
                    redirect_info = resp.get("redirect_url", "")
                    # Check if it's a trailing slash redirect
                    if (original_url.rstrip('/') + '/' == redirect_info or 
                        original_url.rstrip('/') == redirect_info.rstrip('/')):
                        redirect_info += " (Trailing slash redirect)"
                
                lastmod = headers.get("Last-Modified", "")
                if lastmod:
                    try:
                        lastmod_dt = pd.to_datetime(lastmod, errors="coerce", utc=True)
                        lastmod = lastmod_dt.date().isoformat() if not pd.isna(lastmod_dt) else ""
                    except:
                        lastmod = ""
                
                title = meta_desc = canonical = ""
                if status and 200 <= status < 300 and "text/html" in resp.get("content_type", "").lower():
                    title, meta_desc, canonical, _ = parse_page(root, final_url, text)
                elif resp.get("final_status") and 200 <= resp.get("final_status") < 300 and text:
                    # Handle redirected content
                    title, meta_desc, canonical, _ = parse_page(root, final_url, text)
                
                rows.append({
                    "url": original_url, "status": status, "depth": None,
                    "title": title, "meta_description": meta_desc, "canonical": canonical,
                    "last_modified": lastmod, "content_type": resp.get("content_type", ""),
                    "error": resp.get("error", ""),
                    "redirect_url": redirect_info or resp.get("redirect_url", ""),
                    "final_status": resp.get("final_status", status)
                })
                
                if progress_cb:
                    progress_cb(i + j + 1, total)
    
    return pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# -------------------------
# SEO analysis (optimized)
# -------------------------
def enrich_seo_analysis(df: pd.DataFrame, root_seed: str = "") -> pd.DataFrame:
    d = df.copy()
    
    # Vectorized operations for better performance
    d["title_norm"] = d["title"].fillna("").str.strip().str.lower()
    d["meta_desc_norm"] = d["meta_description"].fillna("").str.strip().str.lower()
    
    # Calculate duplicates efficiently
    title_counts = d["title_norm"].value_counts()
    desc_counts = d["meta_desc_norm"].value_counts()
    
    d["title_missing"] = d["title_norm"] == ""
    d["meta_description_missing"] = d["meta_desc_norm"] == ""
    d["title_duplicate"] = d["title_norm"].map(title_counts) > 1
    d["meta_description_duplicate"] = d["meta_desc_norm"].map(desc_counts) > 1
    
    # Canonical analysis with trailing slash awareness
    d["canonical_norm"] = d["canonical"].fillna("").apply(lambda x: normalize_url(x, preserve_trailing_slash=True) if x else "")
    d["canonical_missing"] = d["canonical_norm"] == ""
    
    # Enhanced canonical checks with trailing slash awareness
    def check_canonical_mismatch(row):
        if not row["canonical_norm"]:
            return False
        # Compare URLs with trailing slash preservation
        url_norm = normalize_url(row["url"], preserve_trailing_slash=True)
        canonical_norm = row["canonical_norm"]
        
        # Check if they're different (considering trailing slash variations)
        if url_norm != canonical_norm:
            # Allow for trailing slash differences as valid
            url_no_slash = url_norm.rstrip('/')
            canonical_no_slash = canonical_norm.rstrip('/')
            return url_no_slash != canonical_no_slash
        return False
    
    d["canonical_mismatch"] = d.apply(check_canonical_mismatch, axis=1)
    
    if root_seed:
        root_seed = normalize_url(root_seed, preserve_trailing_slash=True)
        d["canonical_is_external"] = d["canonical_norm"].apply(
            lambda can: not same_site(can, root_seed) if can else False)
    else:
        d["canonical_is_external"] = False
    
    # Add trailing slash analysis
    d["has_trailing_slash"] = d["url"].str.endswith("/") & (d["url"] != d["url"].str.rstrip("/") + "/")
    d["redirect_is_trailing_slash"] = d["redirect_url"].str.contains("Trailing slash redirect", na=False)
    
    # SEO score (same as before)
    d["seo_issues"] = (
        d["title_missing"].astype(int) + 
        d["meta_description_missing"].astype(int) +
        d["title_duplicate"].astype(int) + 
        d["meta_description_duplicate"].astype(int) +
        d["canonical_is_external"].astype(int)
    )
    
    d["is_healthy"] = (
        d["status"].between(200, 299) & 
        (d["seo_issues"] == 0)
    )
    
    return d[["url", "status", "depth", "last_modified", "content_type", "error",
             "title", "title_missing", "title_duplicate", 
             "meta_description", "meta_description_missing", "meta_description_duplicate",
             "canonical", "canonical_missing", "canonical_mismatch", "canonical_is_external",
             "seo_issues", "is_healthy", "redirect_url", "final_status", 
             "has_trailing_slash", "redirect_is_trailing_slash"]]

# -------------------------
# Multiple export formats
# -------------------------
def export_sitemap_xml(df: pd.DataFrame) -> bytes:
    good = df[(df["status"].between(200, 299)) & 
              (df["content_type"].str.contains("text/html", case=False, na=False))]
    
    urlset = ET.Element("urlset", {"xmlns": "http://www.sitemaps.org/schemas/sitemap/0.9"})
    
    for _, row in good.iterrows():
        url_elem = ET.SubElement(urlset, "url")
        ET.SubElement(url_elem, "loc").text = row["url"]
        ET.SubElement(url_elem, "lastmod").text = row.get("last_modified") or today_iso()
        
        depth = row.get("depth", 0)
        priority = max(0.1, 1.0 - min(int(depth or 0), 9) * 0.1)
        ET.SubElement(url_elem, "priority").text = f"{priority:.1f}"
        ET.SubElement(url_elem, "changefreq").text = "weekly"
    
    tree = ET.ElementTree(urlset)
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue()

def export_json(df: pd.DataFrame) -> str:
    data = {
        "generated": datetime.datetime.now().isoformat(),
        "total_urls": len(df),
        "healthy_urls": int(df["is_healthy"].sum()),
        "urls": df.to_dict("records")
    }
    return json.dumps(data, indent=2, ensure_ascii=False)

def export_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        # Summary sheet
        summary = pd.DataFrame({
            "Metric": ["Total URLs", "2xx Status", "SEO Issues", "Healthy URLs", 
                      "Missing Titles", "Missing Descriptions", "Duplicate Titles"],
            "Count": [
                len(df),
                int(df["status"].between(200, 299).sum()),
                int((df["seo_issues"] > 0).sum()),
                int(df["is_healthy"].sum()),
                int(df["title_missing"].sum()),
                int(df["meta_description_missing"].sum()),
                int(df["title_duplicate"].sum())
            ]
        })
        summary.to_excel(writer, sheet_name="Summary", index=False)
        
        # Full data
        df.to_excel(writer, sheet_name="SEO Analysis", index=False)
        
        # Issues only
        issues = df[df["seo_issues"] > 0]
        if len(issues) > 0:
            issues.to_excel(writer, sheet_name="Issues Found", index=False)
    
    return buf.getvalue()

def parse_sitemap_xml(xml_bytes: bytes) -> list:
    """Parse sitemap XML and preserve original URL format including trailing slashes"""
    try:
        root = ET.fromstring(xml_bytes)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locs = []
        
        if root.tag.endswith("urlset"):
            locs = [url.findtext("sm:loc", "", ns).strip() 
                   for url in root.findall(".//sm:url", ns)]
        elif root.tag.endswith("sitemapindex"):
            locs = [smap.findtext("sm:loc", "", ns).strip() 
                   for smap in root.findall(".//sm:sitemap", ns)]
        
        # Preserve original URLs as-is from sitemap (don't normalize yet)
        return [loc for loc in locs if loc]
    except ET.ParseError:
        return []

# -------------------------
# Filter Functions
# -------------------------
def apply_filters(df: pd.DataFrame, status_filter: str, issue_filter: str) -> pd.DataFrame:
    """Apply filters to dataframe without modifying original"""
    filtered_df = df.copy()
    
    # Apply status filter
    if status_filter == "2xx Success":
        filtered_df = filtered_df[filtered_df["status"].between(200, 299)]
    elif status_filter == "3xx Redirects":
        filtered_df = filtered_df[filtered_df["status"].between(300, 399)]
    elif status_filter == "4xx Client Error":
        filtered_df = filtered_df[filtered_df["status"].between(400, 499)]
    elif status_filter == "5xx Server Error":
        filtered_df = filtered_df[filtered_df["status"].between(500, 599)]
    elif status_filter == "Other":
        filtered_df = filtered_df[~filtered_df["status"].between(200, 599)]
    
    # Apply issue filter
    if issue_filter == "No Issues":
        filtered_df = filtered_df[filtered_df["seo_issues"] == 0]
    elif issue_filter == "Has SEO Issues":
        filtered_df = filtered_df[filtered_df["seo_issues"] > 0]
    elif issue_filter == "Missing Title":
        filtered_df = filtered_df[filtered_df["title_missing"]]
    elif issue_filter == "Missing Description":
        filtered_df = filtered_df[filtered_df["meta_description_missing"]]
    elif issue_filter == "Trailing Slash Redirects":
        if 'redirect_is_trailing_slash' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["redirect_is_trailing_slash"]]
    elif issue_filter == "Has Trailing Slash":
        if 'has_trailing_slash' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["has_trailing_slash"]]
    
    return filtered_df

def initialize_filters():
    """Initialize filter state"""
    if 'status_filter' not in st.session_state:
        st.session_state.status_filter = "All"
    if 'issue_filter' not in st.session_state:
        st.session_state.issue_filter = "All"

# -------------------------
# Streamlit UI (Enhanced)
# -------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide", 
                       initial_sidebar_state="expanded")
    
    st.title(APP_TITLE)
    st.markdown("🚀 **Generate & analyze sitemaps up to 1 lakh URLs with multiple export formats**")
    
    # Initialize filters
    initialize_filters()
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        max_pages = st.number_input("Max pages", 1, MAX_URLS, 10000, 1000, 
                                   help=f"Maximum {MAX_URLS:,} URLs supported")
        max_depth = st.number_input("Max depth", 1, 10, 5, 1)
        concurrency = st.number_input("Concurrency", 50, 300, DEFAULT_CONCURRENCY, 10,
                                     help="Higher = faster but more resource intensive")
        
        st.info("🚀 **Speed Tips:**\n- Higher concurrency = faster crawling\n- Reduce max depth for speed\n- Large sites may take time")
        
        # Speed preset buttons
        if st.button("🐌 Conservative (50)", help="Safe for slow servers"):
            st.session_state.concurrency = 50
        if st.button("⚡ Fast (100)", help="Good balance"):
            st.session_state.concurrency = 100  
        if st.button("🚄 Ultra Fast (200)", help="Maximum speed"):
            st.session_state.concurrency = 200
    
    # Main interface
    mode = st.radio("🎯 **Choose Mode**", 
                    ["🕷️ Crawl Website", "✅ Validate URLs/Sitemap"],
                    horizontal=True)
    
    if mode == "🕷️ Crawl Website":
        st.subheader("Website Crawler")
        seed = st.text_input("🌐 **Website URL**", placeholder="https://example.com")
        
        if st.button("🚀 Start Crawling", type="primary"):
            if not seed:
                st.error("Please enter a website URL")
                return
            
            # Clear existing results when starting new crawl
            if 'validation_results' in st.session_state:
                del st.session_state.validation_results
            
            # Reset filters when new data is loaded
            st.session_state.status_filter = "All"
            st.session_state.issue_filter = "All"
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(done, total):
                pct = min(100, int(100 * done / max(1, total)))
                progress_bar.progress(pct)
                status_text.text(f"Crawled {done:,} / {total:,} pages ({pct}%)")
            
            try:
                with st.spinner("🔍 Crawling website..."):
                    df = asyncio.run(crawl_site_enhanced(
                        seed, max_pages, max_depth, concurrency, update_progress))
                    df = enrich_seo_analysis(df, seed)
                
                st.success(f"✅ Successfully crawled {len(df):,} pages!")
                
                # Store results in session state
                st.session_state.crawl_results = df
                display_results_and_exports(df, "crawl")
                
            except Exception as e:
                st.error(f"❌ Crawling failed: {str(e)}")
    
    else:  # Validate mode
        st.subheader("URL/Sitemap Validator")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            uploaded = st.file_uploader("📁 **Upload sitemap.xml**", type=["xml"])
        
        with col2:
            paste_count = st.number_input("URLs to paste", 1, 10000, 100, 50)
        
        pasted = st.text_area(
            "📝 **Or paste URLs** (one per line)", 
            height=200, 
            placeholder="https://example.com/\nhttps://example.com/about\nhttps://example.com/contact",
            help=f"Paste up to {paste_count:,} URLs"
        )
        
        # Process URLs
        urls = []
        if uploaded:
            xml_bytes = uploaded.read()
            urls = parse_sitemap_xml(xml_bytes)
            st.info(f"📊 Found {len(urls):,} URLs in uploaded sitemap")
        
        if pasted.strip():
            pasted_urls = [line.strip() for line in pasted.splitlines() 
                          if line.strip()][:paste_count]
            urls.extend(pasted_urls)
            if len(pasted_urls) == paste_count:
                st.warning(f"⚠️ Limited to {paste_count:,} pasted URLs")
        
        urls = list(dict.fromkeys(urls))  # Remove duplicates
        
        if st.button("🔍 Validate URLs", type="primary"):
            if not urls:
                st.error("Please upload a sitemap or paste some URLs")
                return
            
            # Clear existing results when starting new validation
            if 'crawl_results' in st.session_state:
                del st.session_state.crawl_results
            
            # Reset filters when new data is loaded
            st.session_state.status_filter = "All"
            st.session_state.issue_filter = "All"
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(done, total):
                pct = min(100, int(100 * done / max(1, total)))
                progress_bar.progress(pct)
                status_text.text(f"Validated {done:,} / {total:,} URLs ({pct}%)")
            
            try:
                with st.spinner(f"🔍 Validating {len(urls):,} URLs..."):
                    df = asyncio.run(validate_urls_enhanced(
                        urls, concurrency, update_progress))
                    root_guess = urls[0] if urls else ""
                    df = enrich_seo_analysis(df, root_guess)
                
                st.success(f"✅ Successfully validated {len(df):,} URLs!")
                
                # Store results in session state
                st.session_state.validation_results = df
                display_results_and_exports(df, "validation")
                
            except Exception as e:
                st.error(f"❌ Validation failed: {str(e)}")
    
    # Display existing results if available
    if 'crawl_results' in st.session_state and mode == "🕷️ Crawl Website":
        st.subheader("📊 Previous Crawl Results")
        display_results_and_exports(st.session_state.crawl_results, "crawl")
    
    if 'validation_results' in st.session_state and mode == "✅ Validate URLs/Sitemap":
        st.subheader("📊 Previous Validation Results")
        display_results_and_exports(st.session_state.validation_results, "validation")

def display_results_and_exports(df: pd.DataFrame, mode_prefix: str = ""):
    """Display results and export options with persistent filters"""
    
    # Key metrics
    st.subheader("📊 Overview")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("Total URLs", f"{len(df):,}")
    with col2:
        success_count = int(df["status"].between(200, 299).sum())
        st.metric("2xx Success", f"{success_count:,}")
    with col3:
        redirect_count = int(df["status"].between(300, 399).sum())
        st.metric("3xx Redirects", f"{redirect_count:,}")
    with col4:
        client_error_count = int(df["status"].between(400, 499).sum())
        st.metric("4xx Errors", f"{client_error_count:,}")
    with col5:
        seo_issues = int((df["seo_issues"] > 0).sum())
        st.metric("SEO Issues", f"{seo_issues:,}")
    with col6:
        healthy = int(df["is_healthy"].sum())
        st.metric("Healthy URLs", f"{healthy:,}")
    
    # Data preview with persistent filters
    st.subheader("🔍 Results Preview")
    
    # Filter options with unique keys
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        status_options = ["All", "2xx Success", "3xx Redirects", "4xx Client Error", "5xx Server Error", "Other"]
        status_filter = st.selectbox(
            "Filter by Status", 
            options=status_options,
            index=status_options.index(st.session_state.status_filter),
            key=f"status_filter_{mode_prefix}"
        )
        # Update session state immediately
        st.session_state.status_filter = status_filter
    
    with col2:
        issue_options = ["All", "No Issues", "Has SEO Issues", "Missing Title", "Missing Description", "Trailing Slash Redirects", "Has Trailing Slash"]
        issue_filter = st.selectbox(
            "Filter by Issues", 
            options=issue_options,
            index=issue_options.index(st.session_state.issue_filter),
            key=f"issue_filter_{mode_prefix}"
        )
        # Update session state immediately
        st.session_state.issue_filter = issue_filter
    
    with col3:
        # Clear filters button with unique key
        if st.button("🔄 Clear All Filters", key=f"clear_filters_{mode_prefix}"):
            st.session_state.status_filter = "All"
            st.session_state.issue_filter = "All"
            st.rerun()
    
    # Apply filters using current session state
    filtered_df = apply_filters(df, st.session_state.status_filter, st.session_state.issue_filter)
    
    # Display filtered data
    st.dataframe(filtered_df, use_container_width=True, height=400)
    
    # Filter info caption
    filter_info = []
    if st.session_state.status_filter != "All":
        filter_info.append(f"Status: {st.session_state.status_filter}")
    if st.session_state.issue_filter != "All":
        filter_info.append(f"Issues: {st.session_state.issue_filter}")
    
    filter_text = f" (Filtered by: {', '.join(filter_info)})" if filter_info else ""
    st.caption(f"Showing {len(filtered_df):,} of {len(df):,} URLs{filter_text}")
    
    # Export section
    st.subheader("📥 Export Options")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        xml_data = export_sitemap_xml(filtered_df)  # Export filtered data
        st.download_button(
            "📄 Download XML Sitemap",
            data=xml_data,
            file_name="sitemap.xml",
            mime="application/xml",
            help="Standard XML sitemap format (filtered data)",
            key=f"xml_download_{mode_prefix}"
        )
    
    with col2:
        csv_data = filtered_df.to_csv(index=False).encode("utf-8")  # Export filtered data
        st.download_button(
            "📊 Download CSV Report",
            data=csv_data,
            file_name="seo_report.csv",
            mime="text/csv",
            help="Complete filtered data in CSV format",
            key=f"csv_download_{mode_prefix}"
        )
    
    with col3:
        # Create JSON data with filtered results
        json_data_filtered = {
            "generated": datetime.datetime.now().isoformat(),
            "total_urls": len(filtered_df),
            "healthy_urls": int(filtered_df["is_healthy"].sum()),
            "filters_applied": {
                "status_filter": st.session_state.status_filter,
                "issue_filter": st.session_state.issue_filter
            },
            "urls": filtered_df.to_dict("records")
        }
        json_str = json.dumps(json_data_filtered, indent=2, ensure_ascii=False)
        st.download_button(
            "🔗 Download JSON Data",
            data=json_str.encode("utf-8"),
            file_name="sitemap_data.json",
            mime="application/json",
            help="Structured JSON with metadata (filtered data)",
            key=f"json_download_{mode_prefix}"
        )
    
    with col4:
        excel_data = export_excel(filtered_df)  # Export filtered data
        st.download_button(
            "📈 Download Excel Report",
            data=excel_data,
            file_name="seo_analysis.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Multi-sheet Excel with summary (filtered data)",
            key=f"excel_download_{mode_prefix}"
        )
    
    # Additional info about exports
    if filter_info:
        st.info(f"💡 **Note:** All exports contain only the filtered data ({len(filtered_df):,} URLs). Clear filters to export all data.")
    
    # Quick stats for filtered data
    if len(filtered_df) != len(df):
        st.subheader("🎯 Filtered Data Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Filtered URLs", f"{len(filtered_df):,}", 
                     delta=f"{len(filtered_df) - len(df):,}")
        with col2:
            filtered_2xx = int(filtered_df["status"].between(200, 299).sum())
            original_2xx = int(df["status"].between(200, 299).sum())
            st.metric("2xx in Filter", f"{filtered_2xx:,}", 
                     delta=f"{filtered_2xx - original_2xx:,}")
        with col3:
            filtered_issues = int((filtered_df["seo_issues"] > 0).sum())
            original_issues = int((df["seo_issues"] > 0).sum())
            st.metric("SEO Issues", f"{filtered_issues:,}", 
                     delta=f"{filtered_issues - original_issues:,}")
        with col4:
            filtered_healthy = int(filtered_df["is_healthy"].sum())
            original_healthy = int(df["is_healthy"].sum())
            st.metric("Healthy URLs", f"{filtered_healthy:,}", 
                     delta=f"{filtered_healthy - original_healthy:,}")

st.markdown(
    """
    <style>
    footer {visibility: hidden;}
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: transparent;
        text-align: center;
        font-size: 14px;
        padding: 10px;
        color: #888;
    }
    .footer a {
        color: #888;
        text-decoration: none;
    }
    .footer a:hover {
        text-decoration: underline;
    }
    </style>
    <div class="footer">
        Developed by <a href="https://www.linkedin.com/in/amal-alexander-305780131/" target="_blank">Amal Alexander</a>
    </div>
    """,
    unsafe_allow_html=True
)

if __name__ == "__main__":
    main()