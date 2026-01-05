#!/usr/bin/env python3
"""
Download fund-related SEC filings that contain LPAs, side letters, etc.

Uses sec-edgar-downloader to get filings from:
- Major PE/hedge fund firms
- Investment companies
- BDCs (Business Development Companies)
"""

import os
import re
import shutil
from sec_edgar_downloader import Downloader

OUTPUT_PATH = "./fund_formation_matters"
TEMP_PATH = "./sec_fund_filings"

# Investment managers and fund sponsors known for fund filings
INVESTMENT_COMPANIES = [
    # Major PE firms
    "KKR",          # KKR & Co
    "BX",           # Blackstone
    "APO",          # Apollo Global
    "CG",           # Carlyle Group
    "ARES",         # Ares Management
    "OWL",          # Blue Owl Capital
    "TPG",          # TPG Inc

    # BDCs (file fund docs as exhibits)
    "ARCC",         # Ares Capital
    "MAIN",         # Main Street Capital
    "GBDC",         # Golub Capital BDC
    "HTGC",         # Hercules Capital
    "BXSL",         # Blackstone Secured Lending
    "OBDC",         # Blue Owl Capital Corp
    "PSEC",         # Prospect Capital

    # Other fund managers
    "OAK",          # Oaktree Specialty Lending
    "ORCC",         # Owl Rock Capital
    "TPVG",         # TriplePoint Venture Growth
]

# Filing types that commonly contain fund formation documents
FILING_TYPES = [
    "8-K",      # Material agreements, side letters
    "10-K",     # Annual reports with exhibit lists
    "S-1",      # Registration statements with fund docs
    "S-11",     # Real estate fund registrations
]


def clean_html_to_text(content):
    """Basic HTML tag removal for readability."""
    # Remove script and style blocks
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

    # Replace common entities
    content = content.replace('&nbsp;', ' ')
    content = content.replace('&amp;', '&')
    content = content.replace('&lt;', '<')
    content = content.replace('&gt;', '>')
    content = content.replace('&quot;', '"')

    return content


def find_fund_docs_in_filing(filing_path):
    """
    Look through a filing's files for fund-related documents.
    Returns list of (filepath, doc_type) tuples.
    """
    fund_docs = []
    keywords = {
        'side_letter': ['side letter', 'side-letter', 'sideletter'],
        'lpa': ['limited partnership agreement', 'lp agreement', 'partnership agreement'],
        'subscription': ['subscription agreement', 'subscription document'],
        'ppm': ['private placement', 'offering memorandum', 'confidential memorandum'],
        'investment_mgmt': ['investment management agreement', 'advisory agreement'],
    }

    for root, dirs, files in os.walk(filing_path):
        for filename in files:
            filepath = os.path.join(root, filename)

            # Check if it's a document file
            if not filename.endswith(('.htm', '.html', '.txt')):
                continue

            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(50000).lower()  # Read first 50KB

                # Check for fund document keywords
                for doc_type, kw_list in keywords.items():
                    for kw in kw_list:
                        if kw in content:
                            fund_docs.append((filepath, doc_type, filename))
                            break

            except Exception:
                continue

    return fund_docs


def download_and_extract_fund_docs():
    """Download SEC filings and extract fund-related documents."""
    print("=" * 60)
    print("SEC FUND FORMATION DOCUMENTS EXTRACTOR")
    print("=" * 60)

    # Initialize downloader
    dl = Downloader("LegalResearch", "research@university.edu", TEMP_PATH)

    # Create output folders
    for doc_type in ['Side_Letters', 'LPAs', 'Subscription_Agreements', 'Investment_Mgmt_Agreements']:
        os.makedirs(os.path.join(OUTPUT_PATH, doc_type), exist_ok=True)

    total_found = {
        'side_letter': 0,
        'lpa': 0,
        'subscription': 0,
        'ppm': 0,
        'investment_mgmt': 0,
    }

    for ticker in INVESTMENT_COMPANIES:
        print(f"\n--- Processing {ticker} ---")

        for filing_type in FILING_TYPES:
            try:
                print(f"  Downloading {filing_type} filings...")
                dl.get(filing_type, ticker, limit=3, download_details=True)

                # Look through downloaded filings
                ticker_path = os.path.join(TEMP_PATH, "sec-edgar-filings", ticker, filing_type)

                if not os.path.exists(ticker_path):
                    continue

                for accession in os.listdir(ticker_path):
                    filing_path = os.path.join(ticker_path, accession)
                    if not os.path.isdir(filing_path):
                        continue

                    fund_docs = find_fund_docs_in_filing(filing_path)

                    for filepath, doc_type, filename in fund_docs:
                        # Determine output folder
                        if doc_type == 'side_letter':
                            out_folder = 'Side_Letters'
                        elif doc_type == 'lpa':
                            out_folder = 'LPAs'
                        elif doc_type == 'subscription':
                            out_folder = 'Subscription_Agreements'
                        elif doc_type == 'investment_mgmt':
                            out_folder = 'Investment_Mgmt_Agreements'
                        else:
                            continue

                        # Copy file
                        out_name = f"{ticker}_{accession[:10]}_{filename}"
                        out_path = os.path.join(OUTPUT_PATH, out_folder, out_name)

                        if not os.path.exists(out_path):
                            shutil.copy2(filepath, out_path)
                            print(f"    Found {doc_type}: {filename[:40]}")
                            total_found[doc_type] += 1

            except Exception as e:
                print(f"  Error with {filing_type}: {e}")
                continue

    # Summary
    print(f"\n{'=' * 60}")
    print("EXTRACTION COMPLETE")
    print(f"{'=' * 60}")
    for doc_type, count in total_found.items():
        print(f"  {doc_type}: {count} documents")


if __name__ == "__main__":
    download_and_extract_fund_docs()
