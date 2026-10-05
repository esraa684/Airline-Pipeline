import os
import sys
import time
import argparse
import urllib.request
import urllib.error
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
DEFAULT_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

def download_and_extract_month(year, month, data_dir, retries=3):
    """
    Downloads the monthly zip from BTS, extracts the CSV, and removes the zip.
    Skips if CSV already exists and is valid.
    """
    os.makedirs(data_dir, exist_ok=True)
    
    # Check if any matching CSV already exists in data_dir
    expected_csv_pattern = f"_{year}_{month}.csv"
    existing_files = [f for f in os.listdir(data_dir) if f.endswith(expected_csv_pattern)]
    if existing_files:
        existing_path = os.path.join(data_dir, existing_files[0])
        size_mb = os.path.getsize(existing_path) / (1024 * 1024)
        if size_mb > 50:  # Valid BTS monthly CSV is > 100 MB
            print(f"[{year}-{month:02d}] ALREADY EXISTS: {existing_files[0]} ({size_mb:.1f} MB) - Skipping.")
            return True, year, month, size_mb, 0.0

    url = BASE_URL.format(year=year, month=month)
    temp_zip = os.path.join(data_dir, f"temp_{year}_{month}.zip")
    
    for attempt in range(1, retries + 1):
        try:
            start_t = time.time()
            print(f"[{year}-{month:02d}] Downloading... (Attempt {attempt}/{retries})")
            
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=120) as resp, open(temp_zip, 'wb') as out_f:
                while True:
                    chunk = resp.read(1024 * 1024) # 1 MB chunks
                    if not chunk:
                        break
                    out_f.write(chunk)
            
            download_duration = time.time() - start_t
            zip_size_mb = os.path.getsize(temp_zip) / (1024 * 1024)
            
            # Extract CSV
            extracted_csv_name = None
            csv_size_mb = 0
            with zipfile.ZipFile(temp_zip, 'r') as z:
                for item in z.infolist():
                    if item.filename.endswith(".csv"):
                        extracted_path = z.extract(item, data_dir)
                        extracted_csv_name = os.path.basename(extracted_path)
                        csv_size_mb = os.path.getsize(extracted_path) / (1024 * 1024)
            
            # Remove temporary zip
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
            
            # Remove any extracted readme.html if present
            readme_path = os.path.join(data_dir, "readme.html")
            if os.path.exists(readme_path):
                try:
                    os.remove(readme_path)
                except Exception:
                    pass
            
            speed = (zip_size_mb / download_duration) if download_duration > 0 else 0
            print(f"[{year}-{month:02d}] SUCCESS: Extracted {extracted_csv_name} ({csv_size_mb:.1f} MB) in {download_duration:.1f}s ({speed:.2f} MB/s)")
            return True, year, month, csv_size_mb, download_duration

        except Exception as e:
            print(f"[{year}-{month:02d}] WARNING on attempt {attempt}: {e}")
            if os.path.exists(temp_zip):
                try:
                    os.remove(temp_zip)
                except Exception:
                    pass
            if attempt < retries:
                time.sleep(3 * attempt)
            else:
                print(f"[{year}-{month:02d}] FAILED after {retries} attempts.")
                return False, year, month, 0, 0

def run_batch(year, data_dir, workers=3):
    print(f"\n{'='*70}")
    print(f" STARTING BATCH FOR YEAR {year}")
    print(f" Target folder: {data_dir}")
    print(f" Concurrency  : {workers} workers")
    print(f"{'='*70}\n")
    
    batch_start = time.time()
    months = list(range(1, 13))
    total_extracted_mb = 0
    success_count = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(download_and_extract_month, year, m, data_dir): m for m in months}
        for future in as_completed(futures):
            success, y, m, size_mb, dur = future.result()
            if success:
                success_count += 1
                total_extracted_mb += size_mb
    
    batch_time = time.time() - batch_start
    print(f"\n{'-'*70}")
    print(f" BATCH {year} COMPLETE:")
    print(f" Successfully processed: {success_count}/12 months")
    print(f" Total extracted CSV size: {total_extracted_mb:.1f} MB ({total_extracted_mb/1024:.2f} GB)")
    print(f" Batch elapsed time: {batch_time/60:.2f} minutes")
    print(f"{'-'*70}\n")
    return success_count == 12

def main():
    parser = argparse.ArgumentParser(description="Download and extract BTS Airline On-Time Performance data in batches.")
    parser.add_argument("--year", type=str, default="2022", help="Year to download (2022, 2023, 2024, 2025, or 'all')")
    parser.add_argument("--workers", type=int, default=3, help="Number of concurrent download threads (default: 3)")
    parser.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR, help="Destination directory for raw CSVs")
    args = parser.parse_args()

    print(f"BTS Data Downloader Initialized.")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Destination: {args.data_dir}")
    
    if args.year.lower() == "all":
        years = [2022, 2023, 2024, 2025]
    else:
        years = [int(args.year)]
    
    grand_start = time.time()
    for y in years:
        run_batch(y, args.data_dir, workers=args.workers)
        
    total_time = time.time() - grand_start
    print(f"\n=======================================================")
    print(f" ALL REQUESTED BATCHES FINISHED in {total_time/60:.2f} minutes!")
    print(f"=======================================================\n")

if __name__ == "__main__":
    main()
