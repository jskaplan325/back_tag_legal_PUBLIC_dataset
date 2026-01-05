#!/usr/bin/env python3
"""
Download Side Letters and Fund Documents from SEC EDGAR

Searches for side letter agreements and limited partnership agreements
filed as exhibits (EX-10, EX-99) in SEC filings.
"""

import os
import re
import time
import requests
from pathlib import Path

OUTPUT_PATH = "./fund_formation_matters"

# SEC requires specific headers
HEADERS = {
    'User-Agent': 'Legal Research Dataset legal@university.edu',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov',
}


def search_edgar_fulltext(query, form_types=None, max_results=50):
    """
    Search SEC EDGAR using the full-text search API.
    Returns list of (filing_url, document_url, title) tuples.
    """
    results = []

    # SEC EDGAR Full Text Search API
    search_url = "https://efts.sec.gov/LATEST/search-index"

    params = {
        'q': query,
        'dateRange': 'custom',
        'startdt': '2015-01-01',
        'enddt': '2025-12-31',
        'page': 1,
        'from': 0,
        'size': max_results,
    }

    if form_types:
        params['forms'] = ','.join(form_types)

    try:
        response = requests.get(search_url, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])

            for hit in hits:
                source = hit.get('_source', {})
                file_url = source.get('file_url', '')
                form_type = source.get('form', '')
                company = source.get('display_names', [''])[0]

                if file_url:
                    results.append({
                        'url': f"https://www.sec.gov{file_url}",
                        'company': company,
                        'form': form_type,
                    })

    except Exception as e:
        print(f"  Search error: {e}")

    return results


def download_with_sec_headers(url, dest_path):
    """Download from SEC with proper headers."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()

        with open(dest_path, 'wb') as f:
            f.write(response.content)

        return True
    except Exception as e:
        print(f"    Failed: {e}")
        return False


def search_and_download_side_letters():
    """Search for and download side letter documents."""
    print("\n" + "=" * 60)
    print("SEARCHING SEC EDGAR FOR SIDE LETTERS")
    print("=" * 60)

    folder_path = os.path.join(OUTPUT_PATH, "Side_Letters")
    os.makedirs(folder_path, exist_ok=True)

    # Search queries for side letters
    queries = [
        '"side letter" "limited partner"',
        '"form of side letter"',
        '"side letter agreement" investor',
    ]

    all_results = []
    for query in queries:
        print(f"\nSearching: {query}")
        results = search_edgar_fulltext(query, form_types=['8-K', '10-K', 'S-1', 'EX-10'])
        all_results.extend(results)
        print(f"  Found {len(results)} results")
        time.sleep(1)

    # Deduplicate by URL
    seen_urls = set()
    unique_results = []
    for r in all_results:
        if r['url'] not in seen_urls:
            seen_urls.add(r['url'])
            unique_results.append(r)

    print(f"\nTotal unique results: {len(unique_results)}")

    # Download top results
    downloaded = 0
    for i, result in enumerate(unique_results[:20]):
        company = result['company'][:30].replace(' ', '_').replace(',', '')
        company = re.sub(r'[^\w\-]', '', company)
        filename = f"Side_Letter_{company}_{i+1}.html"
        dest_path = os.path.join(folder_path, filename)

        print(f"  Downloading: {result['company'][:40]}...")
        if download_with_sec_headers(result['url'], dest_path):
            downloaded += 1
        time.sleep(1)

    return downloaded


def search_and_download_lpas():
    """Search for and download LPA documents."""
    print("\n" + "=" * 60)
    print("SEARCHING SEC EDGAR FOR LIMITED PARTNERSHIP AGREEMENTS")
    print("=" * 60)

    folder_path = os.path.join(OUTPUT_PATH, "LPAs")
    os.makedirs(folder_path, exist_ok=True)

    queries = [
        '"limited partnership agreement" "private equity"',
        '"limited partnership agreement" "hedge fund"',
        '"agreement of limited partnership" fund',
    ]

    all_results = []
    for query in queries:
        print(f"\nSearching: {query}")
        results = search_edgar_fulltext(query, form_types=['8-K', '10-K', 'S-1', 'EX-99'])
        all_results.extend(results)
        print(f"  Found {len(results)} results")
        time.sleep(1)

    # Deduplicate
    seen_urls = set()
    unique_results = []
    for r in all_results:
        if r['url'] not in seen_urls:
            seen_urls.add(r['url'])
            unique_results.append(r)

    print(f"\nTotal unique results: {len(unique_results)}")

    # Download top results
    downloaded = 0
    for i, result in enumerate(unique_results[:20]):
        company = result['company'][:30].replace(' ', '_').replace(',', '')
        company = re.sub(r'[^\w\-]', '', company)
        filename = f"LPA_{company}_{i+1}.html"
        dest_path = os.path.join(folder_path, filename)

        print(f"  Downloading: {result['company'][:40]}...")
        if download_with_sec_headers(result['url'], dest_path):
            downloaded += 1
        time.sleep(1)

    return downloaded


def search_and_download_subscription_docs():
    """Search for subscription agreement documents."""
    print("\n" + "=" * 60)
    print("SEARCHING SEC EDGAR FOR SUBSCRIPTION AGREEMENTS")
    print("=" * 60)

    folder_path = os.path.join(OUTPUT_PATH, "Subscription_Agreements")
    os.makedirs(folder_path, exist_ok=True)

    queries = [
        '"subscription agreement" "limited partner"',
        '"form of subscription" fund investor',
    ]

    all_results = []
    for query in queries:
        print(f"\nSearching: {query}")
        results = search_edgar_fulltext(query, form_types=['8-K', 'S-1', 'EX-10'])
        all_results.extend(results)
        print(f"  Found {len(results)} results")
        time.sleep(1)

    # Deduplicate
    seen_urls = set()
    unique_results = []
    for r in all_results:
        if r['url'] not in seen_urls:
            seen_urls.add(r['url'])
            unique_results.append(r)

    print(f"\nTotal unique results: {len(unique_results)}")

    # Download top results
    downloaded = 0
    for i, result in enumerate(unique_results[:15]):
        company = result['company'][:30].replace(' ', '_').replace(',', '')
        company = re.sub(r'[^\w\-]', '', company)
        filename = f"Subscription_{company}_{i+1}.html"
        dest_path = os.path.join(folder_path, filename)

        print(f"  Downloading: {result['company'][:40]}...")
        if download_with_sec_headers(result['url'], dest_path):
            downloaded += 1
        time.sleep(1)

    return downloaded


def main():
    print("=" * 60)
    print("SEC EDGAR FUND DOCUMENTS DOWNLOADER")
    print("(Side Letters, LPAs, Subscription Agreements)")
    print("=" * 60)

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    side_letters = search_and_download_side_letters()
    lpas = search_and_download_lpas()
    subscriptions = search_and_download_subscription_docs()

    print(f"\n{'=' * 60}")
    print("DOWNLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"Side Letters:           {side_letters} documents")
    print(f"LPAs:                   {lpas} documents")
    print(f"Subscription Agreements: {subscriptions} documents")
    print(f"{'=' * 60}")
    print(f"Total:                  {side_letters + lpas + subscriptions} documents")


if __name__ == "__main__":
    main()
