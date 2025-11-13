import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import json
import os
from collections import Counter, defaultdict

# helper function for report (to be implented)
def _save_analytics_snapshot():
    analytics_data = {"unique_urls": list(visited_urls),"word_freq": dict(word_frequency),"subdomains": dict(subdomain_counter),"longest_page": longest_page,}
    os.makedirs("analytics", exist_ok=True)
    with open("analytics/stats.json", "w", encoding="utf-8") as f:
        json.dump(analytics_data, f, indent=2)

# report stats
visited_urls = set()
word_frequency = Counter()
subdomain_counter = defaultdict(int)
longest_page = {"url": None, "word_count": 0}

# restricted domains to visit
ALLOWED_DOMAINS = {"ics.uci.edu","cs.uci.edu","informatics.uci.edu","stat.uci.edu",}

# stop words to ignore (gathered from ranks.nl)
STOP_WORDS = {"a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can't","cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
    "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having",
    "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no",
    "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't",
    "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
    "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's",
    "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll",
    "you're", "you've", "your", "yours", "yourself", "yourselves"}

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
    
    # don't process large files
    try:
        clen = int(resp.raw_response.headers.get("Content-Length", 0))
        if clen > 8_000_000:
            return extracted_urls
    except Exception:
        pass

    # HTML parser
    try:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception:
        return extracted_urls

    # get text characters
    text = soup.get_text(" ", strip=True)

    # avoiding pages with less than 250 characters (no info)
    if len(text) < 250:
        return extracted_urls

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
        path = parsed.path or ""
        query = parsed.query or ""

        if not any(hostname == d or hostname.endswith("." + d) for d in ALLOWED_DOMAINS):
            return False
     
        if hostname in {"wiki.ics.uci.edu", "swiki.ics.uci.edu"} and "doku.php" in path:
            if "do=media" in query or "do=export" in query:
                return False
            if "image=" in query and ("tab_files=" in query or "ns=" in query):
                return False

        low_path_query = (path + "?" + query) if query else path
        trap_substrings = ("/calendar", "/events", "/event", "/archives", "/archive", "/feed", "format=feed", "view=print", "print=1", "preview=", "share=", "replytocom=", "utm", "sessionid", "phpsessid")

        if any(s in low_path_query.lower() for s in trap_substrings):
            return False

         #trap checkers
        if "calendar" in parsed.netloc:
            return False

        date_pattern = re.compile(r'\d{4}[-/]\d{2}[-/]\d{2}')
        if date_pattern.search(parsed.path):
            return False

        if re.match(r'(page|date|year|month)=\d{4,}', parsed.query):
            return False

        if len(query) > 120 or query.count("&") > 6:
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
