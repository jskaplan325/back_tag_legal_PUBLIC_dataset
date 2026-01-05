#!/usr/bin/env python3
"""
Expanded SEC Fund Documents Downloader

Searches a broader set of investment managers, PE firms, hedge funds,
and BDCs for fund formation documents including side letters.
"""

import os
import re
import shutil
import time
from sec_edgar_downloader import Downloader

OUTPUT_PATH = "./fund_formation_matters"
TEMP_PATH = "./sec_fund_filings_expanded"

# Expanded list of investment companies and fund managers
INVESTMENT_COMPANIES = {
    # Major PE Firms
    "PE_Firms": [
        "KKR",          # KKR & Co
        "BX",           # Blackstone
        "APO",          # Apollo Global
        "CG",           # Carlyle Group
        "ARES",         # Ares Management
        "TPG",          # TPG Inc
        "EQT",          # EQT AB (if listed)
        "BAM",          # Brookfield Asset Management
        "BN",           # Brookfield Corp
    ],

    # Business Development Companies (BDCs) - rich source of fund docs
    "BDCs": [
        "ARCC",         # Ares Capital
        "MAIN",         # Main Street Capital
        "GBDC",         # Golub Capital BDC
        "HTGC",         # Hercules Capital
        "BXSL",         # Blackstone Secured Lending
        "OBDC",         # Blue Owl Capital Corp
        "PSEC",         # Prospect Capital
        "FSK",          # FS KKR Capital
        "TPVG",         # TriplePoint Venture
        "GSBD",         # Goldman Sachs BDC
        "NMFC",         # New Mountain Finance
        "TCPC",         # BlackRock TCP Capital
        "CSWC",         # Capital Southwest
        "GLAD",         # Gladstone Capital
        "GAIN",         # Gladstone Investment
        "FDUS",         # Fidus Investment
        "PNNT",         # PennantPark Floating Rate
        "PFLT",         # PennantPark Floating Rate Capital
        "SLRC",         # SLR Investment
        "MFIC",         # MidCap Financial
        "BBDC",         # Barings BDC
        "OCSL",         # Oaktree Specialty Lending
        "TRIN",         # Trinity Capital
        "CCAP",         # Crescent Capital BDC
    ],

    # Asset Managers with fund products
    "Asset_Managers": [
        "BLK",          # BlackRock
        "SCHW",         # Schwab
        "TROW",         # T Rowe Price
        "IVZ",          # Invesco
        "BEN",          # Franklin Resources
        "AMG",          # Affiliated Managers
        "VCTR",         # Victory Capital
        "AB",           # AllianceBernstein
        "JHG",          # Janus Henderson
        "CNS",          # Cohen & Steers
        "APAM",         # Artisan Partners
        "VRTS",         # Virtus Investment
        "STEP",         # StepStone Group
        "HLI",          # Houlihan Lokey (M&A advisor)
        "EVR",          # Evercore
        "PJT",          # PJT Partners
        "GHL",          # Greenhill
        "MC",           # Moelis
        "LAZ",          # Lazard
    ],

    # SPACs and Acquisition Corps (often have fund-like structures)
    "SPACs": [
        "IPOF",         # Social Capital Hedosophia
        "PSTH",         # Pershing Square Tontine
        "CCIV",         # Churchill Capital
    ],

    # Insurance/Alt Asset Managers
    "Insurance_Alt": [
        "AMP",          # Ameriprise
        "PFG",          # Principal Financial
        "VOYA",         # Voya Financial
        "EQH",          # Equitable Holdings
        "ATH",          # Athene
    ],
}

# Filing types that commonly contain fund formation documents
FILING_TYPES = [
    "8-K",          # Material agreements, side letters, amendments
    "10-K",         # Annual reports with full exhibit lists
    "10-Q",         # Quarterly with material agreements
    "S-1",          # IPO registration - often has fund docs
    "S-11",         # Real estate fund registrations
    "N-2",          # Closed-end fund registration
    "485BPOS",      # Post-effective amendments (registered funds)
    "DEF 14A",      # Proxy statements sometimes have agreements
]

# Keywords to identify fund-related documents
FUND_DOC_KEYWORDS = {
    'side_letter': [
        'side letter', 'side-letter', 'sideletter',
        'letter agreement', 'investor letter',
        'supplemental agreement', 'letter of understanding',
        'preferential terms', 'mfn', 'most favored nation',
    ],
    'lpa': [
        'limited partnership agreement', 'lp agreement',
        'partnership agreement', 'agreement of limited partnership',
        'amended and restated limited partnership',
        'llc agreement', 'operating agreement of',
        'limited liability company agreement',
    ],
    'subscription': [
        'subscription agreement', 'subscription document',
        'investor subscription', 'capital commitment agreement',
        'commitment agreement', 'joinder agreement',
    ],
    'investment_mgmt': [
        'investment management agreement', 'investment advisory agreement',
        'advisory agreement', 'management agreement',
        'sub-advisory agreement', 'portfolio management',
    ],
    'fund_admin': [
        'administration agreement', 'fund administration',
        'custodian agreement', 'custody agreement',
        'prime brokerage agreement', 'transfer agent',
    ],
    'ppm': [
        'private placement memorandum', 'confidential memorandum',
        'offering memorandum', 'private offering',
        'confidential offering', 'information memorandum',
    ],
}


def find_fund_docs_in_filing(filing_path, verbose=False):
    """
    Look through a filing's files for fund-related documents.
    Returns list of (filepath, doc_type, filename) tuples.
    """
    fund_docs = []

    for root, dirs, files in os.walk(filing_path):
        for filename in files:
            filepath = os.path.join(root, filename)

            # Check document files
            if not any(filename.lower().endswith(ext) for ext in ['.htm', '.html', '.txt', '.xml']):
                continue

            # Skip index files
            if 'index' in filename.lower() or filename.startswith('.'):
                continue

            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(100000).lower()  # Read first 100KB

                # Also check filename
                filename_lower = filename.lower()

                # Check for fund document keywords
                for doc_type, kw_list in FUND_DOC_KEYWORDS.items():
                    for kw in kw_list:
                        if kw in content or kw.replace(' ', '') in filename_lower or kw.replace(' ', '-') in filename_lower:
                            # Verify it's actually a document (not just a mention)
                            if len(content) > 5000:  # Substantial document
                                fund_docs.append((filepath, doc_type, filename))
                                if verbose:
                                    print(f"      Found {doc_type}: {filename[:50]}")
                            break
                    else:
                        continue
                    break  # Only classify as one type

            except Exception as e:
                continue

    return fund_docs


def get_output_folder(doc_type):
    """Map document type to output folder."""
    mapping = {
        'side_letter': 'Side_Letters',
        'lpa': 'LPAs',
        'subscription': 'Subscription_Agreements',
        'investment_mgmt': 'Investment_Mgmt_Agreements',
        'fund_admin': 'Fund_Admin_Agreements',
        'ppm': 'Private_Placement_Memos_SEC',
    }
    return mapping.get(doc_type, 'Other_Fund_Docs')


def download_and_extract():
    """Download SEC filings and extract fund-related documents."""
    print("=" * 70)
    print("EXPANDED SEC FUND FORMATION DOCUMENTS EXTRACTOR")
    print("=" * 70)

    # Initialize downloader
    dl = Downloader("LegalResearchDataset", "research@university.edu", TEMP_PATH)

    # Create output folders
    for folder in ['Side_Letters', 'LPAs', 'Subscription_Agreements',
                   'Investment_Mgmt_Agreements', 'Fund_Admin_Agreements',
                   'Private_Placement_Memos_SEC', 'Other_Fund_Docs']:
        os.makedirs(os.path.join(OUTPUT_PATH, folder), exist_ok=True)

    # Track results
    total_found = {k: 0 for k in FUND_DOC_KEYWORDS.keys()}
    processed_companies = 0
    failed_companies = []

    # Process each category
    for category, tickers in INVESTMENT_COMPANIES.items():
        print(f"\n{'=' * 70}")
        print(f"CATEGORY: {category} ({len(tickers)} companies)")
        print("=" * 70)

        for ticker in tickers:
            processed_companies += 1
            print(f"\n[{processed_companies}] Processing {ticker}...")

            company_found = 0

            for filing_type in FILING_TYPES:
                try:
                    # Download filings (limit to recent ones)
                    dl.get(filing_type, ticker, limit=5, download_details=True)

                    # Look through downloaded filings
                    ticker_path = os.path.join(TEMP_PATH, "sec-edgar-filings", ticker, filing_type)

                    if not os.path.exists(ticker_path):
                        continue

                    for accession in os.listdir(ticker_path):
                        filing_path = os.path.join(ticker_path, accession)
                        if not os.path.isdir(filing_path):
                            continue

                        fund_docs = find_fund_docs_in_filing(filing_path, verbose=True)

                        for filepath, doc_type, filename in fund_docs:
                            out_folder = get_output_folder(doc_type)
                            out_name = f"{ticker}_{filing_type}_{accession[:10]}_{filename}"
                            # Clean filename
                            out_name = re.sub(r'[<>:"/\\|?*]', '_', out_name)
                            out_path = os.path.join(OUTPUT_PATH, out_folder, out_name)

                            if not os.path.exists(out_path):
                                shutil.copy2(filepath, out_path)
                                total_found[doc_type] += 1
                                company_found += 1

                except Exception as e:
                    if "invalid" not in str(e).lower():
                        print(f"    Error with {filing_type}: {str(e)[:50]}")
                    continue

            if company_found > 0:
                print(f"    => Found {company_found} fund documents")

            # Brief pause to be respectful to SEC servers
            time.sleep(0.5)

    # Summary
    print(f"\n{'=' * 70}")
    print("EXTRACTION COMPLETE")
    print(f"{'=' * 70}")
    print(f"\nDocuments found by type:")
    for doc_type, count in total_found.items():
        if count > 0:
            print(f"  {doc_type}: {count}")
    print(f"\nTotal: {sum(total_found.values())} documents")

    # Show folder contents
    print(f"\nFolder summary:")
    for folder in sorted(os.listdir(OUTPUT_PATH)):
        folder_path = os.path.join(OUTPUT_PATH, folder)
        if os.path.isdir(folder_path):
            count = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
            if count > 0:
                print(f"  {folder}: {count} files")


if __name__ == "__main__":
    download_and_extract()
