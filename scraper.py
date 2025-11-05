import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import json
import os
from collections import Counter, defaultdict
import requests

# helper function for report (to be implented)
def _save_analytics_snapshot():
    analytics_data = {"unique_urls": list(visited_urls),"word_freq": dict(word_frequency),"subdomains": dict(subdomain_counter),"longest_page": longest_page,}
    os.makedirs("analytics", exist_ok=True)
    with open("analytics/stats.json", "w", encoding="utf-8") as f:
        json.dump(analytics_data, f, indent=2)

#report stats
visited_urls = set()
word_frequency = Counter()
subdomain_counter = defaultdict(int)
longest_page = {"url": None, "word_count": 0}

# restricted domains to visit
ALLOWED_DOMAINS = {"ics.uci.edu","cs.uci.edu","informatics.uci.edu","stat.uci.edu",}

# stop words to ignore (gathered from ranks.nl)
STOP_WORDS = STOP_WORDS = set(requests.get("https://www.ranks.nl/stopwords").text.splitlines())

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    extracted_urls = []

    # check for valid response status
    if resp.status != 200 or resp.raw_response is None:
        return extracted_urls

    # only process valid HTML files 
    content_type = resp.raw_response.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        return extracted_urls

    # HTML parser
    try:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception:
        return extracted_urls

    # get text 
    text = soup.get_text(" ", strip=True)

    # normalize and uniform to lower case and alpnum
    tokens = [t.lower() for t in re.findall(r"[a-zA-Z0-9]+", text)]

    # filter out stop words
    tokens = [t for t in tokens if t not in STOP_WORDS]

    # remove fragments 
    defragmented_url, _ = urldefrag(resp.url or url)

    # add URL not visited
    if defragmented_url not in visited_urls:
        visited_urls.add(defragmented_url)

        word_frequency.update(tokens)

        word_count = len(tokens)
        if word_count > longest_page["word_count"]:
            longest_page["url"] = defragmented_url
            longest_page["word_count"] = word_count

        hostname = urlparse(defragmented_url).hostname or ""
        if hostname.endswith(".uci.edu") or hostname == "uci.edu":
            subdomain_counter[hostname] += 1

        if len(visited_urls) % 25 == 0:
            _save_analytics_snapshot()

    # extract and normalize links
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not href or href.startswith(("mailto:", "javascript:")):
            continue
        normalized_link, _ = urldefrag(urljoin(defragmented_url, href))
        extracted_urls.append(normalized_link)

    return extracted_urls

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # only allow valid subdomains
        hostname = parsed.hostname or ""
        if not any(hostname == d or hostname.endswith("." + d) for d in ALLOWED_DOMAINS):
            return False

        # avoiding traps
        query = parsed.query or ""
        path = parsed.path or ""
        trap_patterns = [
            re.compile(r"(calendar|ical|event)", re.I),
            re.compile(r"(sessionid|phpsessid|utm_)", re.I),
            re.compile(r"(page=\d{3,}|offset=\d{3,})", re.I),
        ]

        # rejecting URLs with long queries / a lot of parameters
        if len(query) > 120 or query.count("&") > 6:
            return False
        for pattern in trap_patterns:
            if pattern.search(path + query):
                return False

        # skip non-HTML files
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", (parsed.path or "").lower()
        )

    except TypeError:
        print("TypeError for ", parsed)
        raise
