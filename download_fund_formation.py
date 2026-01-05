#!/usr/bin/env python3
"""
Download Fund Formation Documents Dataset

Sources:
1. UVA Legal Data Lab - Hedge fund operating agreements & PPMs
2. SEC EDGAR - LPAs, side letters, subscription agreements filed as exhibits
3. ILPA - Model LPA templates

All files preserved in original format (PDF/HTML).
"""

import os
import re
import time
import requests
from pathlib import Path
from urllib.parse import urljoin, quote

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
OUTPUT_PATH = "./fund_formation_matters"
TEMP_PATH = "./fund_temp"

HEADERS = {
    'User-Agent': 'Legal Dataset Builder (educational/research purposes)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# UVA Legal Data Lab - Hedge Fund Documents
UVA_BASE_URL = "https://legaldatalab.law.virginia.edu/hedge_funds/pdf"

UVA_DOCUMENTS = {
    "Operating_Agreements": [
        ("Amaranth_Partners", "oa/Amaranth_2003.pdf"),
        ("Bear_Stearns_High_Grade_Structured_Credit", "oa/Bear_Stearns_High-Grade_Structured_Credit_Strategies_Master_Fund.pdf"),
        ("Bear_Stearns_Leveraged_2x", "oa/Bear_Stearns_High-Grade_Structured_Credit_Strategies_Leveraged_2x_Master_Fund.pdf"),
        ("Exponential_Returns", "oa/Exponential_Returns.pdf"),
        ("FM_Multi_Strategy", "oa/FM_MULTI-STRATEGY_INVESTMENT_FUND.pdf"),
        ("Genesis_Market_Neutral", "oa/Genesis_Market_Neutral_Index_Fund.pdf"),
        ("Integral_Arbitrage", "oa/Integral_Arbitrage.pdf"),
        ("Integral_Hedging", "oa/Integral_Hedging.pdf"),
        ("Jade_Trading", "oa/Jade_Trading.pdf"),
        ("Lancer_Offshore", "oa/Lancer_Offshore.pdf"),
        ("Last_Atlantis", "oa/Last_Atlantis.pdf"),
        ("Lipco_Partners", "oa/Lipco_Partners.pdf"),
        ("LSPV", "oa/LSPV.pdf"),
        ("Mercator_Momentum", "oa/Mercator_Momentum_Fund.pdf"),
        ("Paramount_Partners", "oa/Paramount_Partners.pdf"),
        ("Rye_Select_Broad_Market_XL_Fund", "oa/Rye_Select_Broad_Market_XL_Fund.pdf"),
        ("Rye_Select_Broad_Market_XL_Portfolio", "oa/Rye_Select_Broad_Market_XL_Portfolio_Ltd.pdf"),
        ("Securion_I", "oa/Securion_I.pdf"),
        ("Sum_It_Investments", "oa/Sum-It_Investments.pdf"),
        ("Vestium_Equity_Funds", "oa/Vestium_Equity_Funds.pdf"),
    ],
    "Private_Placement_Memos": [
        ("Amaranth_Partners_2006", "ppm/Amaranth_2006.pdf"),
        ("Apex_Equity_Options", "ppm/Apex_Equity_Option_Funds.pdf"),
        ("Ark_Discovery_Fund", "ppm/Ark_Discovery_Fund.pdf"),
        ("Exponential_Returns_1996", "ppm/Exponential_Returns.pdf"),
        ("Fairfield_Sentry_2003", "ppm/Fairfield_Sentry_2003.pdf"),
        ("Fairfield_Sentry_2004", "ppm/Fairfield_Sentry_2004.pdf"),
        ("Fairfield_Sentry_2006", "ppm/Fairfield_Sentry.pdf"),
        ("Fairfield_Sigma_2008", "ppm/Fairfield_Sigma.pdf"),
        ("Finvest_Primer_2005", "ppm/Finvest_Primer.pdf"),
        ("Finvest_Yankee_2007", "ppm/Finvest_Yankee.pdf"),
        ("First_Frontier_1999", "ppm/First_Frontier.pdf"),
        ("FM_Multi_Strategy_2000", "ppm/FM_MULTI-STRATEGY_INVESTMENT_FUND.pdf"),
        ("Genesis_Market_Neutral_1998", "ppm/Genesis_Market_Neutral_Index_Fund.pdf"),
        ("Greenwich_Sentry_Partners_2006", "ppm/Greenwich_Sentry_Partners.pdf"),
        ("Greenwich_Sentry_1998", "ppm/Greenwich_Sentry_1998.pdf"),
        ("Greenwich_Sentry_2006", "ppm/Greenwich_Sentry_2006.pdf"),
        ("Income_Plus_2003", "ppm/Income-Plus_Investment_Fund_2003.pdf"),
        ("Income_Plus_1993", "ppm/Income-Plus_Investment_Fund.pdf"),
        ("Integral_Arbitrage_2001", "ppm/Integral_Arbitrage.pdf"),
        ("Integral_Hedging_1999", "ppm/Integral_Hedging.pdf"),
        ("Lancer_Offshore_2002", "ppm/Lancer Offshore.pdf"),
        ("Lancer_Partners_2001", "ppm/Lancer_Partners.pdf"),
        ("Last_Atlantis_2007", "ppm/Last_Atlantis.pdf"),
        ("Lipper_Convertibles_1997", "ppm/Lipper_Convertibles_1997.pdf"),
        ("Lipper_Convertibles_2000", "ppm/Lipper_Convertibles_2000.pdf"),
        ("Mayur_2008", "ppm/Mayur.pdf"),
        ("MDL_Active_Duration_2003", "ppm/MDL_Active_Duration_Fund_2003-1.pdf"),
        ("Mercator_Momentum_2006", "ppm/Mercator_Momentum_Fund.pdf"),
        ("Omnifund_2002", "ppm/Omnifund.pdf"),
        ("Paramount_Partners_2005", "ppm/Paramount_Partners_2005.pdf"),
        ("Paramount_Partners_2007", "ppm/Paramount_Partners_2007.pdf"),
        ("Rye_Select_XL_Fund_2007", "ppm/Rye_Select_Broad_Market_XL_Fund.pdf"),
        ("Rye_Select_XL_Portfolio_2008", "ppm/Rye_Select_Broad_Market_XL_Portfolio_Ltd.pdf"),
        ("Securion_I_2007", "ppm/Securion_I.pdf"),
        ("Sum_It_Investments_2000", "ppm/Sum-It_Investments.pdf"),
        ("Vestium_Equity_2008", "ppm/Vestium_Equity_Funds.pdf"),
    ],
}

# SEC EDGAR - Side letters and fund documents filed as exhibits
SEC_EDGAR_DOCUMENTS = {
    "Side_Letters": [
        # Known side letter exhibits from SEC filings
        ("Side_Letter_Example_1", "https://www.sec.gov/Archives/edgar/data/1096934/000072174816001857/site1229168kex10_2.htm"),
        ("Side_Letter_TPG", "https://www.sec.gov/Archives/edgar/data/1880661/000119312522057998/d278498dex103.htm"),
    ],
    "LPAs": [
        # Limited Partnership Agreements from SEC
        ("LPA_Master_Fund_Example", "https://www.sec.gov/Archives/edgar/data/1597218/000119312514060638/d667770dex99a1.htm"),
        ("LPA_Fund_Example_2", "https://www.sec.gov/Archives/edgar/data/1597220/000119312514060680/d667850dex99a1.htm"),
    ],
    "Subscription_Agreements": [
        # Subscription document examples
    ],
}

# ILPA Model Documents
ILPA_DOCUMENTS = {
    "Model_LPAs": [
        ("ILPA_Model_LPA_Whole_of_Fund", "https://ilpa.org/wp-content/uploads/2023/09/ILPA-Model-LPA-Whole-of-Fund-Word.docx"),
        ("ILPA_Model_LPA_Deal_by_Deal", "https://ilpa.org/wp-content/uploads/2023/09/ILPA-Model-LPA-Deal-by-Deal-Word.docx"),
    ],
}


def download_file(url, dest_path, description=""):
    """Download a file with retry logic."""
    max_retries = 3

    for attempt in range(max_retries):
        try:
            # Handle URL encoding for spaces
            if ' ' in url:
                url = url.replace(' ', '%20')

            response = requests.get(url, headers=HEADERS, timeout=30, stream=True)
            response.raise_for_status()

            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_kb = os.path.getsize(dest_path) / 1024
            print(f"    Downloaded: {description} ({size_kb:.1f} KB)")
            return True

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"    Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(2)
            else:
                print(f"    Failed: {description} - {e}")
                return False

    return False


def download_uva_documents():
    """Download hedge fund documents from UVA Legal Data Lab."""
    print("\n" + "=" * 60)
    print("DOWNLOADING UVA LEGAL DATA LAB - HEDGE FUND DOCUMENTS")
    print("=" * 60)

    total_downloaded = 0

    for doc_type, documents in UVA_DOCUMENTS.items():
        folder_path = os.path.join(OUTPUT_PATH, doc_type)
        os.makedirs(folder_path, exist_ok=True)

        print(f"\n--- {doc_type} ({len(documents)} files) ---")

        for name, path in documents:
            url = f"{UVA_BASE_URL}/{path}"
            ext = os.path.splitext(path)[1]
            dest_file = os.path.join(folder_path, f"{name}{ext}")

            if os.path.exists(dest_file):
                print(f"    Skipped (exists): {name}")
                total_downloaded += 1
                continue

            if download_file(url, dest_file, name):
                total_downloaded += 1

            time.sleep(0.5)  # Be respectful to the server

    return total_downloaded


def download_sec_documents():
    """Download fund formation documents from SEC EDGAR."""
    print("\n" + "=" * 60)
    print("DOWNLOADING SEC EDGAR - FUND FORMATION EXHIBITS")
    print("=" * 60)

    total_downloaded = 0

    for doc_type, documents in SEC_EDGAR_DOCUMENTS.items():
        if not documents:
            continue

        folder_path = os.path.join(OUTPUT_PATH, doc_type)
        os.makedirs(folder_path, exist_ok=True)

        print(f"\n--- {doc_type} ({len(documents)} files) ---")

        for name, url in documents:
            # Determine extension from URL
            if url.endswith('.htm') or url.endswith('.html'):
                ext = '.html'
            elif url.endswith('.pdf'):
                ext = '.pdf'
            else:
                ext = '.html'

            dest_file = os.path.join(folder_path, f"{name}{ext}")

            if os.path.exists(dest_file):
                print(f"    Skipped (exists): {name}")
                total_downloaded += 1
                continue

            if download_file(url, dest_file, name):
                total_downloaded += 1

            time.sleep(1)  # Respect SEC rate limits

    return total_downloaded


def download_ilpa_documents():
    """Download ILPA model LPA documents."""
    print("\n" + "=" * 60)
    print("DOWNLOADING ILPA - MODEL LPA TEMPLATES")
    print("=" * 60)

    total_downloaded = 0

    for doc_type, documents in ILPA_DOCUMENTS.items():
        folder_path = os.path.join(OUTPUT_PATH, doc_type)
        os.makedirs(folder_path, exist_ok=True)

        print(f"\n--- {doc_type} ({len(documents)} files) ---")

        for name, url in documents:
            ext = os.path.splitext(url)[1]
            dest_file = os.path.join(folder_path, f"{name}{ext}")

            if os.path.exists(dest_file):
                print(f"    Skipped (exists): {name}")
                total_downloaded += 1
                continue

            if download_file(url, dest_file, name):
                total_downloaded += 1

            time.sleep(0.5)

    return total_downloaded


def search_sec_for_side_letters():
    """
    Search SEC EDGAR for additional side letter filings.
    Uses the SEC full-text search API.
    """
    print("\n" + "=" * 60)
    print("SEARCHING SEC EDGAR FOR SIDE LETTERS")
    print("=" * 60)

    # SEC EFTS (Electronic Full Text Search) API
    search_url = "https://efts.sec.gov/LATEST/search-index"

    queries = [
        '"side letter" "limited partner"',
        '"side letter agreement" fund',
        '"form of side letter"',
    ]

    found_docs = []
    folder_path = os.path.join(OUTPUT_PATH, "Side_Letters_SEC")
    os.makedirs(folder_path, exist_ok=True)

    for query in queries:
        try:
            params = {
                'q': query,
                'dateRange': 'custom',
                'startdt': '2020-01-01',
                'enddt': '2025-12-31',
                'forms': 'EX-10,8-K,S-1,10-K',
            }

            # Use the search page
            search_api = "https://www.sec.gov/cgi-bin/srch-ia"
            response = requests.get(search_api, params={'text': query, 'first': 1, 'last': 20},
                                   headers=HEADERS, timeout=30)

            if response.status_code == 200:
                print(f"  Searched: {query[:50]}...")

            time.sleep(2)

        except Exception as e:
            print(f"  Search error: {e}")

    return len(found_docs)


def main():
    print("=" * 60)
    print("FUND FORMATION DOCUMENTS DATASET BUILDER")
    print("(Original formats preserved - PDF, DOCX, HTML)")
    print("=" * 60)

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Download from each source
    uva_count = download_uva_documents()
    sec_count = download_sec_documents()
    ilpa_count = download_ilpa_documents()

    # Optional: Search for more side letters
    # search_sec_for_side_letters()

    # Summary
    print(f"\n{'=' * 60}")
    print("DOWNLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"UVA Legal Data Lab:  {uva_count} documents")
    print(f"SEC EDGAR:           {sec_count} documents")
    print(f"ILPA Model Docs:     {ilpa_count} documents")
    print(f"{'=' * 60}")
    print(f"Total:               {uva_count + sec_count + ilpa_count} documents")
    print(f"\nSaved to: {OUTPUT_PATH}/")

    # List folder structure
    print(f"\nFolder structure:")
    for folder in sorted(os.listdir(OUTPUT_PATH)):
        folder_path = os.path.join(OUTPUT_PATH, folder)
        if os.path.isdir(folder_path):
            count = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
            print(f"  {folder}/: {count} files")


if __name__ == "__main__":
    main()
