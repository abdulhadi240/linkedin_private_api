import gspread
import requests
import time
import unicodedata
import logging
import ast  # Abstract Syntax Tree module to safely evaluate string literals
import re
from typing import Dict, Tuple, Optional, List
from oauth2client.service_account import ServiceAccountCredentials

# === LOGGING SETUP ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === CONFIGURATION ===
CONFIG = {
    "service_account_file": "service_account.json",
    "spreadsheet_accounts": {
        "name": "LinkedIn Accounts",
        "sheet": "LinkedIn Accounts"
    },
    "spreadsheet_urls": {
        "name": "Test Linkedin Data",
        "sheet_urls": "Sheet1",
        "sheet_results": "Results"
    },
    "scrape_api": "https://linkedin-private.chitlangia.co"
}

# === UTILITY FUNCTIONS ===
def get_gsheet_client():
    """Authorizes and returns a gspread client."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["service_account_file"], scope)
        return gspread.authorize(creds)
    except FileNotFoundError:
        logger.error(f"Service account file not found at: {CONFIG['service_account_file']}")
        return None
    except Exception as e:
        logger.error(f"Failed to authorize with Google Sheets: {e}")
        return None

def parse_cookie(cookie_str: str) -> Dict[str, str]:
    """
    Safely parses a string representation of a dictionary (JSON-like)
    into a dictionary object and cleans the values.
    """
    try:
        # ast.literal_eval safely evaluates a string containing a Python literal
        parsed_dict = ast.literal_eval(cookie_str)
        if not isinstance(parsed_dict, dict):
            logger.warning("Parsed data is not a dictionary.")
            return {}
        
        # Clean the values by stripping extra quotes
        cleaned_dict = {key: str(val).strip('"') for key, val in parsed_dict.items()}
        return cleaned_dict
    except (ValueError, SyntaxError, TypeError) as e:
        # Fallback for regular cookie strings if ast.literal_eval fails
        logger.info(f"Could not parse as dictionary literal ({e}), trying regex for key=value pairs.")
        return {key.strip(): val.strip('"') for key, val in re.findall(r'([^=;\s]+)=(\".*?\"|[^;]+)', cookie_str)}


def extract_ids(cookie_data: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """Extracts JSESSIONID and li_at values from a cookie dictionary."""
    jsessionid = cookie_data.get('JSESSIONID')
    li_at = cookie_data.get('li_at')
    return jsessionid, li_at

def normalize_status(status: str) -> str:
    """Normalizes a status string for consistent comparison."""
    return unicodedata.normalize("NFKD", status or "").strip().lower()

def read_accounts(client: gspread.Client) -> List[Dict]:
    """
    Reads verified accounts from the 'LinkedIn Accounts' spreadsheet.
    It extracts credentials from the 'cookies' column for rows where
    'verification_status' is 'verified'.
    """
    if not client:
        return []
        
    try:
        sheet = client.open(CONFIG["spreadsheet_accounts"]["name"]).worksheet(CONFIG["spreadsheet_accounts"]["sheet"])
        rows = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Failed to read accounts from Google Sheet: {e}")
        return []

    if not rows or len(rows) < 2:
        logger.warning("No data found in 'LinkedIn Accounts' sheet.")
        return []

    header = rows[0]
    try:
        session_idx = header.index("cookies")
        proxy_idx = header.index("ip_and_port")
        status_idx = header.index("verification_status")
        proxy_idx = header.index("Structured proxy")
    except ValueError as e:
        logger.error(f"A required column is missing from the 'LinkedIn Accounts' sheet: {e}")
        return []

    accounts = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) <= max(session_idx, proxy_idx, status_idx):
            logger.warning(f"Row {i} is incomplete and will be skipped.")
            continue

        status = normalize_status(row[status_idx])
        if status != "verified":
            logger.info(f"Row {i} skipped: Account status is '{status}', not 'verified'.")
            continue

        session_str = row[session_idx].strip()
        proxy = row[proxy_idx].strip()

        if not session_str:
            logger.warning(f"Row {i} skipped: 'cookies' column is empty.")
            continue

        cookie_data = parse_cookie(session_str)
        jsessionid, li_at = extract_ids(cookie_data)

        if li_at and jsessionid:
            accounts.append({"li_at": li_at, "JSESSIONID": jsessionid, "proxy": proxy})
        else:
            logger.warning(f"Row {i} skipped: Could not extract 'li_at' or 'JSESSIONID' from the cookie data.")

    logger.info(f"Successfully loaded {len(accounts)} verified accounts.")
    return accounts

def read_profile_urls(client: gspread.Client) -> List[str]:
    """Reads profile URLs from the 'Test Linkedin Data' spreadsheet."""
    if not client:
        return []
        
    try:
        sheet = client.open(CONFIG["spreadsheet_urls"]["name"]).worksheet(CONFIG["spreadsheet_urls"]["sheet_urls"])
        rows = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Failed to read profile URLs: {e}")
        return []

    if not rows or len(rows) < 2:
        logger.warning("No URLs found in 'Test Linkedin Data' sheet.")
        return []

    try:
        url_idx = rows[0].index("Corporate Linkedin Url")
    except ValueError:
        logger.error("'Corporate Linkedin Url' column not found.")
        return []

    urls = [row[url_idx].strip() for row in rows[1:] if len(row) > url_idx and row[url_idx].strip()]
    logger.info(f"Successfully loaded {len(urls)} URLs to process.")
    return urls

def write_results(client: gspread.Client, results: List[Dict]):
    """Writes the collected data to the 'Results' sheet."""
    if not results:
        logger.warning("No results to write.")
        return
        
    if not client:
        return

    try:
        sheet = client.open(CONFIG["spreadsheet_urls"]["name"]).worksheet(CONFIG["spreadsheet_urls"]["sheet_results"])
        headers = list(results[0].keys())
        # Ensure all rows have the same keys in the same order
        values = [headers] + [[row.get(h, "") for h in headers] for row in results]

        sheet.clear()
        sheet.update("A1", values, value_input_option='USER_ENTERED')
        logger.info(f"Successfully wrote {len(results)} results to the '{CONFIG['spreadsheet_urls']['sheet_results']}' sheet.")
    except Exception as e:
        logger.error(f"Failed to write results to Google Sheet: {e}")

def split_batches(data: list, size: int) -> list:
    """Splits a list into smaller chunks of a specified size."""
    return [data[i:i + size] for i in range(0, len(data), size)]

def start_scraping(account: Dict, urls: List[str]) -> Optional[str]:
    """Initiates a scraping job via the API."""
    jsessionid = account["JSESSIONID"].replace("ajax:", "")

    payload = {
        "JSESSIONID": jsessionid,
        "li_at": account["li_at"],
        "profile_urls": urls,
        "proxy": account["proxy"]
    }
     # Log the payload (API request body)
    logger.info(f"Sending API request to {CONFIG['scrape_api']}/scrape-linkedin with body:\n{payload}")

    try:
        response = requests.post(f"{CONFIG['scrape_api']}/scrape-linkedin", json=payload, timeout=30)
        response.raise_for_status()
        batch_id = response.json().get("batch_id")
        logger.info(f"Successfully started scraping for batch ID: {batch_id}")
        return batch_id
    except requests.exceptions.RequestException as e:
        logger.error(f"API call to start scraping failed: {e}")
        return None

def wait_for_completion(batch_id: str) -> List[Dict]:
    """Polls the API to check for the completion of a scraping batch."""
    max_retries = 10
    # Wait for 5 minutes (300 seconds) between checks
    wait_interval = 300

    for attempt in range(max_retries):
        try:
            logger.info(f"Checking status for batch {batch_id} (Attempt {attempt + 1}/{max_retries})...")
            response = requests.get(f"{CONFIG['scrape_api']}/scrape-status/{batch_id}", timeout=30)
            response.raise_for_status()
            data = response.json()

            status = data.get("status")
            if status == "completed":
                logger.info(f"Batch {batch_id} completed successfully.")
                return data.get("result", [])
            elif status == "failed":
                logger.error(f"Batch {batch_id} failed. Reason: {data.get('error')}")
                return []
            else: # Status is likely 'pending' or 'in_progress'
                logger.info(f"Batch {batch_id} is still in progress. Waiting for {wait_interval} seconds...")
                time.sleep(wait_interval)

        except requests.exceptions.RequestException as e:
            logger.error(f"Status check for batch {batch_id} failed: {e}")
            
            # Check if it's a client error (4xx) - these are typically not recoverable
            if hasattr(e, 'response') and e.response is not None and 400 <= e.response.status_code < 500:
                logger.error(f"Client error {e.response.status_code} for batch {batch_id}. Moving to next batch.")
                return []
            
            # For other errors (network issues, server errors), wait and retry
            logger.info(f"Retrying status check for batch {batch_id} after {wait_interval} seconds...")
            time.sleep(wait_interval)

    logger.warning(f"Batch {batch_id} did not complete after {max_retries} attempts.")
    return []

# === MAIN EXECUTION ===
if __name__ == "__main__":
    logger.info("--- Starting LinkedIn Scraping Process ---")

    # Initialize the client once and pass it to functions
    gspread_client = get_gsheet_client()
    if not gspread_client:
        logger.error("Execution stopped: Could not authorize with Google Sheets.")
        exit()

    accounts = read_accounts(gspread_client)
    urls = read_profile_urls(gspread_client)

    if not accounts:
        logger.error("Execution stopped: No verified accounts were found.")
        exit()
    
    if not urls:
        logger.error("Execution stopped: No profile URLs were found to process.")
        exit()

    # Split URLs into batches of 50
    batches = split_batches(urls, 100)
    all_results = []

    # Number of batches to process is limited by the number of available accounts
    num_to_process = min(len(accounts), len(batches))
    logger.info(f"Processing {num_to_process} batches with {len(accounts)} available accounts.")

    for i in range(num_to_process):
        account = accounts[i]
        batch_urls = batches[i]

        logger.info(f"--- Processing Batch {i+1}/{num_to_process} with proxy {account['proxy']} ---")
        batch_id = start_scraping(account, batch_urls)

        if batch_id:
            result = wait_for_completion(batch_id)
            if result:
                all_results.extend(result)
            else:
                logger.warning(f"No results obtained for batch {i+1}. Moving to next batch.")
        else:
            logger.warning(f"Failed to start scraping for batch {i+1}. Moving to next batch.")

    if all_results:
        write_results(gspread_client, all_results)
    else:
        logger.warning("No data was collected from any batch.")

    logger.info("--- LinkedIn Scraping Process Finished ---")