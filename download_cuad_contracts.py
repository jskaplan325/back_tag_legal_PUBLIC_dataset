#!/usr/bin/env python3
"""
Download CUAD (Contract Understanding Atticus Dataset) and organize PDFs into matters.

CUAD contains 510 commercial contracts in PDF format from SEC EDGAR filings.
Source: https://huggingface.co/datasets/theatticusproject/cuad-qa

Files are kept in their original PDF format - no conversion to text.
"""

import os
import re
import zipfile
import shutil
import requests
import time
from pathlib import Path

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
OUTPUT_PATH = "./cuad_matters"
TEMP_PATH = "./cuad_temp"

# Try multiple sources
CUAD_SOURCES = [
    # GitHub release
    "https://github.com/TheAtticusProject/cuad/releases/download/v1/CUAD_v1.zip",
    # Zenodo (backup)
    "https://zenodo.org/records/4595826/files/CUAD_v1.zip?download=1",
]

# Practice area mappings based on filename keywords
PRACTICE_AREA_MAPPINGS = {
    "M_and_A": [
        "merger", "acquisition", "asset purchase", "stock purchase",
        "share purchase", "purchase agreement", "combination"
    ],
    "Corporate": [
        "joint venture", "collaboration", "strategic alliance",
        "partnership", "shareholder", "voting agreement", "stockholder"
    ],
    "IP_Licensing": [
        "license", "intellectual property", "patent", "software",
        "trademark", "royalty", "technology"
    ],
    "Commercial": [
        "supply", "distribution", "manufacturing", "service",
        "master service", "outsourcing", "hosting", "maintenance",
        "marketing", "reseller", "agency", "consulting", "vendor"
    ],
    "Employment": [
        "employment", "non-compete", "noncompete", "confidentiality",
        "nda", "non-disclosure", "nondisclosure", "separation", "severance"
    ],
    "Finance": [
        "credit", "loan", "security agreement", "guarantee",
        "note purchase", "indenture", "pledge", "financing"
    ]
}

# Target matters per practice area
TARGET_MATTERS_PER_AREA = 3
DOCS_PER_MATTER = 15


def classify_by_filename(filename):
    """Classify a contract into a practice area based on filename."""
    name_lower = filename.lower()

    for practice_area, keywords in PRACTICE_AREA_MAPPINGS.items():
        for keyword in keywords:
            if keyword in name_lower:
                return practice_area

    # Default to Commercial if no match
    return "Commercial"


def sanitize_filename(name):
    """Create a safe filename."""
    safe = re.sub(r'[<>:"/\\|?*]', '', name)
    safe = re.sub(r'\s+', '_', safe)
    safe = safe.strip('_')
    return safe[:80] if safe else "contract"


# ---------------------------------------------------------
# DOWNLOAD AND EXTRACTION
# ---------------------------------------------------------
def download_cuad_zip():
    """Download CUAD ZIP file, trying multiple sources."""
    os.makedirs(TEMP_PATH, exist_ok=True)
    zip_path = os.path.join(TEMP_PATH, "CUAD_v1.zip")

    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 1000000:
        print(f"CUAD ZIP already downloaded: {zip_path}")
        return zip_path

    print("Downloading CUAD dataset...")
    print("(This is ~200MB and may take a few minutes)\n")

    for url in CUAD_SOURCES:
        print(f"Trying: {url[:60]}...")
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (legal-dataset-downloader)'}
            response = requests.get(url, stream=True, headers=headers, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        pct = (downloaded / total_size) * 100
                        print(f"\r  Downloaded: {downloaded / 1024 / 1024:.1f} MB ({pct:.1f}%)", end="")

            print(f"\n  Saved to: {zip_path}")
            return zip_path

        except requests.exceptions.RequestException as e:
            print(f"  Failed: {e}")
            time.sleep(2)
            continue

    raise Exception("Could not download CUAD from any source")


def extract_cuad_zip(zip_path):
    """Extract the CUAD ZIP file."""
    extract_path = os.path.join(TEMP_PATH, "CUAD_v1")

    if os.path.exists(extract_path):
        print(f"CUAD already extracted: {extract_path}")
        return extract_path

    print(f"Extracting ZIP file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(TEMP_PATH)

    print(f"  Extracted to: {extract_path}")
    return extract_path


def find_files(extract_path, extensions=None):
    """Find all files with given extensions in the extracted directory."""
    if extensions is None:
        extensions = ['.pdf', '.docx', '.doc', '.txt']

    files = []
    for root, dirs, filenames in os.walk(extract_path):
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext in extensions:
                files.append(os.path.join(root, filename))
    return sorted(files)


def save_matter(practice_area, matter_num, doc_paths, output_path):
    """Copy all documents for a matter to its folder, preserving original format."""
    folder_name = f"{practice_area}_{matter_num}"
    save_path = os.path.join(output_path, folder_name)
    os.makedirs(save_path, exist_ok=True)

    print(f"\n--- Saving {folder_name} ({len(doc_paths)} docs) ---")

    for i, src_path in enumerate(doc_paths):
        filename = os.path.basename(src_path)
        dest_path = os.path.join(save_path, filename)

        # Handle duplicates
        if os.path.exists(dest_path):
            base, ext = os.path.splitext(filename)
            filename = f"{base}_{i}{ext}"
            dest_path = os.path.join(save_path, filename)

        shutil.copy2(src_path, dest_path)
        print(f"    {filename}")


# ---------------------------------------------------------
# MAIN PROCESSING
# ---------------------------------------------------------
def main():
    print("=" * 60)
    print("CUAD CONTRACT DATASET DOWNLOADER")
    print("(Preserves original PDF format)")
    print("=" * 60)

    # Download and extract
    zip_path = download_cuad_zip()
    extract_path = extract_cuad_zip(zip_path)

    # Find PDF files (CUAD primarily has PDFs)
    pdf_files = find_files(extract_path, extensions=['.pdf'])
    print(f"\nFound {len(pdf_files)} PDF files")

    if not pdf_files:
        print("No PDF files found!")
        return

    # Initialize matter tracking
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    matter_counts = {area: 0 for area in PRACTICE_AREA_MAPPINGS.keys()}
    matter_docs = {area: {} for area in PRACTICE_AREA_MAPPINGS.keys()}

    # Process each PDF
    print(f"\nOrganizing PDFs into practice area matters...")
    processed = 0

    for pdf_path in pdf_files:
        processed += 1
        filename = os.path.basename(pdf_path)

        # Check if we have enough for all areas
        if all(c >= TARGET_MATTERS_PER_AREA for c in matter_counts.values()):
            print("\n*** Target matters reached for all practice areas! ***")
            break

        # Classify by filename
        practice_area = classify_by_filename(filename)
        if matter_counts[practice_area] >= TARGET_MATTERS_PER_AREA:
            continue

        # Add to matter
        current_matter = matter_counts[practice_area] + 1
        if current_matter not in matter_docs[practice_area]:
            matter_docs[practice_area][current_matter] = []

        matter_docs[practice_area][current_matter].append(pdf_path)
        print(f"  [{practice_area}_{current_matter}] {filename[:50]}...")

        # Check if matter is complete
        if len(matter_docs[practice_area][current_matter]) >= DOCS_PER_MATTER:
            save_matter(practice_area, current_matter,
                       matter_docs[practice_area][current_matter], OUTPUT_PATH)
            matter_counts[practice_area] += 1

    # Save any remaining partial matters
    print("\n--- Saving remaining matters ---")
    for practice_area in matter_docs:
        for matter_num, docs in matter_docs[practice_area].items():
            if docs and matter_num > matter_counts[practice_area]:
                save_matter(practice_area, matter_num, docs, OUTPUT_PATH)
                matter_counts[practice_area] = matter_num

    # Summary
    print(f"\n{'=' * 60}")
    print("CUAD PROCESSING COMPLETE")
    print(f"{'=' * 60}")
    print(f"PDFs processed: {processed}")
    print(f"\nMatters created (original PDF format preserved):")
    total_docs = 0
    for area, count in matter_counts.items():
        if count > 0:
            print(f"  {area}: {count} matter(s)")
            total_docs += sum(len(docs) for docs in matter_docs[area].values())
    print(f"\nTotal documents: {total_docs}")
    print(f"Saved to: {OUTPUT_PATH}/")


if __name__ == "__main__":
    main()
