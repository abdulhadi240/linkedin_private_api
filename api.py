from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, HttpUrl, validator
import gspread
import requests
import time
import unicodedata
import logging
import ast
import re
from typing import Dict, Tuple, Optional, List
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

# === LOGGING SETUP ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === FASTAPI APP ===
app = FastAPI(
    title="LinkedIn Scraper API",
    description="API for scraping LinkedIn profiles with account management and daily usage limits",
    version="1.0.0"
)

# === CONFIGURATION ===
CONFIG = {
    "service_account_file": "service_account.json",
    "spreadsheet_accounts": {
        "name": "LinkedIn Accounts",
        "sheet": "LinkedIn Accounts"
    },
    "scrape_api": "https://linkedin-private.chitlangia.co",
    "daily_limit": 250
}

# === PYDANTIC MODELS ===
class ScrapeRequest(BaseModel):
    linkedin_url: HttpUrl
    
    @validator('linkedin_url')
    def validate_linkedin_url(cls, v):
        url_str = str(v)
        if 'linkedin.com' not in url_str:
            raise ValueError('URL must be a LinkedIn URL')
        return url_str

class ScrapeResponse(BaseModel):
    success: bool
    message: str
    batch_id: Optional[str] = None
    data: Optional[Dict] = None
    account_used: Optional[str] = None

class StatusResponse(BaseModel):
    batch_id: str
    status: str
    result: Optional[List[Dict]] = None
    error: Optional[str] = None

# === GLOBAL VARIABLES ===
gspread_client = None
executor = ThreadPoolExecutor(max_workers=5)

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

def read_available_account(client: gspread.Client) -> Optional[Dict]:
    """
    Reads verified accounts from the 'LinkedIn Accounts' spreadsheet
    and returns the first account with daily usage less than the limit.
    """
    if not client:
        return None
        
    try:
        sheet = client.open(CONFIG["spreadsheet_accounts"]["name"]).worksheet(CONFIG["spreadsheet_accounts"]["sheet"])
        rows = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Failed to read accounts from Google Sheet: {e}")
        return None

    if not rows or len(rows) < 2:
        logger.warning("No data found in 'LinkedIn Accounts' sheet.")
        return None

    header = rows[0]
    try:
        session_idx = header.index("cookies")
        proxy_idx = header.index("Structured proxy")
        status_idx = header.index("verification_status")
        daily_use_idx = header.index("daily_use")
    except ValueError as e:
        logger.error(f"A required column is missing from the 'LinkedIn Accounts' sheet: {e}")
        return None
    
    for i, row in enumerate(rows[1:], start=2):
        if len(row) <= max(session_idx, proxy_idx, status_idx, daily_use_idx):
            logger.warning(f"Row {i} is incomplete and will be skipped.")
            continue

        status = normalize_status(row[status_idx])
        if status != "verified":
            continue

        session_str = row[session_idx].strip()
        proxy = row[proxy_idx].strip()
        
        # Get daily usage
        daily_use_str = row[daily_use_idx].strip()
        try:
            daily_use = int(daily_use_str) if daily_use_str else 0
        except ValueError:
            daily_use = 0

        # Check if account is available (daily usage less than limit)
        if daily_use >= CONFIG["daily_limit"]:
            logger.info(f"Row {i} skipped: Daily usage limit reached ({daily_use}/{CONFIG['daily_limit']})")
            continue

        if not session_str:
            logger.warning(f"Row {i} skipped: 'cookies' column is empty.")
            continue

        cookie_data = parse_cookie(session_str)
        jsessionid, li_at = extract_ids(cookie_data)

        if li_at and jsessionid:
            account = {
                "li_at": li_at, 
                "JSESSIONID": jsessionid, 
                "proxy": proxy,
                "row_index": i,
                "daily_use": daily_use,
                "sheet": sheet
            }
            logger.info(f"Selected account from row {i} with daily usage: {daily_use}/{CONFIG['daily_limit']}")
            return account

    logger.warning("No available verified accounts found with daily usage under the limit.")
    return None

def column_index_to_letter(index: int) -> str:
    """Convert a column index (0-based) to Excel-style column letter (A, B, ..., Z, AA, AB, ...)."""
    result = ""
    while index >= 0:
        result = chr(index % 26 + ord('A')) + result
        index = index // 26 - 1
    return result

def update_account_usage(account: Dict):
    """Updates the daily usage count for the used account."""
    try:
        new_daily_use = account["daily_use"] + 1
        
        # Update daily_use column
        sheet = account["sheet"]
        row_index = account["row_index"]
        
        # Get header to find column indices
        header = sheet.row_values(1)
        daily_use_col_index = header.index("daily_use") + 1  # +1 because gspread uses 1-based indexing
        
        # Method 1: Use update_cell with numeric indices (recommended)
        sheet.update_cell(row_index, daily_use_col_index, str(new_daily_use))
        
        # Method 2: Alternative using proper column letter conversion (commented out)
        # daily_use_col_letter = column_index_to_letter(header.index("daily_use"))
        # sheet.update(f"{daily_use_col_letter}{row_index}", str(new_daily_use))
        
        logger.info(f"Updated account usage: {new_daily_use}/{CONFIG['daily_limit']} for row {row_index}, column {daily_use_col_index}")
    except Exception as e:
        logger.error(f"Failed to update account usage: {e}")
        logger.error(f"Row: {account.get('row_index', 'unknown')}, trying to update daily_use column")

def start_scraping(account: Dict, urls: List[str]) -> Optional[str]:
    """Initiates a scraping job via the API."""
    jsessionid = account["JSESSIONID"].replace("ajax:", "")

    payload = {
        "JSESSIONID": jsessionid,
        "li_at": account["li_at"],
        "profile_urls": urls,
        "proxy": account["proxy"]
    }
    
    logger.info(f"Sending API request to {CONFIG['scrape_api']}/scrape-linkedin")

    try:
        response = requests.post(f"{CONFIG['scrape_api']}/scrape-linkedin", json=payload, timeout=30)
        response.raise_for_status()
        batch_id = response.json().get("batch_id")
        logger.info(f"Successfully started scraping for batch ID: {batch_id}")
        
        # Update account usage after successful API call
        update_account_usage(account)
        
        return batch_id
    except requests.exceptions.RequestException as e:
        logger.error(f"API call to start scraping failed: {e}")
        return None

def wait_for_completion(batch_id: str) -> Dict:
    """Polls the API to check for the completion of a scraping batch."""
    max_retries = 10
    wait_interval = 10  # 5 minutes

    for attempt in range(max_retries):
        try:
            logger.info(f"Checking status for batch {batch_id} (Attempt {attempt + 1}/{max_retries})...")
            response = requests.get(f"{CONFIG['scrape_api']}/scrape-status/{batch_id}", timeout=30)
            response.raise_for_status()
            data = response.json()

            status = data.get("status")
            if status == "completed":
                logger.info(f"Batch {batch_id} completed successfully.")
                return {"status": "completed", "result": data.get("result", []), "error": None}
            elif status == "failed":
                error_msg = data.get('error', 'Unknown error')
                logger.error(f"Batch {batch_id} failed. Reason: {error_msg}")
                return {"status": "failed", "result": None, "error": error_msg}
            else:
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    logger.info(f"Batch {batch_id} is still in progress. Waiting for {wait_interval} seconds...")
                    time.sleep(wait_interval)

        except requests.exceptions.RequestException as e:
            logger.error(f"Status check for batch {batch_id} failed: {e}")
            
            if hasattr(e, 'response') and e.response is not None and 400 <= e.response.status_code < 500:
                logger.error(f"Client error {e.response.status_code} for batch {batch_id}.")
                return {"status": "failed", "result": None, "error": f"Client error: {e.response.status_code}"}
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying status check for batch {batch_id} after {wait_interval} seconds...")
                time.sleep(wait_interval)

    logger.warning(f"Batch {batch_id} did not complete after {max_retries} attempts.")
    return {"status": "timeout", "result": None, "error": "Batch processing timeout"}

# === API ENDPOINTS ===
@app.on_event("startup")
async def startup_event():
    """Initialize the Google Sheets client on startup."""
    global gspread_client
    gspread_client = get_gsheet_client()
    if not gspread_client:
        logger.error("Failed to initialize Google Sheets client")
    else:
        logger.info("Google Sheets client initialized successfully")

@app.get("/")
async def root():
    """Root endpoint providing API information."""
    return {
        "message": "LinkedIn Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "scrape": "/scrape - POST - Scrape a LinkedIn profile",
            "status": "/status/{batch_id} - GET - Check scraping status",
            "health": "/health - GET - Health check"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "gsheet_client": gspread_client is not None,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_linkedin_profile(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Scrape a LinkedIn profile using an available account.
    """
    if not gspread_client:
        raise HTTPException(status_code=500, detail="Google Sheets client not initialized")

    try:
        # Find an available account
        account = read_available_account(gspread_client)
        if not account:
            raise HTTPException(
                status_code=429, 
                detail="No available accounts found. All accounts have reached their daily limit or are not verified."
            )

        # Start scraping process
        urls = [str(request.linkedin_url)]
        batch_id = start_scraping(account, urls)
        
        if not batch_id:
            raise HTTPException(status_code=500, detail="Failed to start scraping process")

        return ScrapeResponse(
            success=True,
            message="Scraping started successfully",
            batch_id=batch_id,
            account_used=f"Row {account['row_index']} (Usage: {account['daily_use'] + 1}/{CONFIG['daily_limit']})"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in scrape endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/scrape-and-wait", response_model=ScrapeResponse)
async def scrape_and_wait_for_result(request: ScrapeRequest):
    """
    Scrape a LinkedIn profile and wait for the result.
    This endpoint will take longer but returns the complete result.
    """
    if not gspread_client:
        raise HTTPException(status_code=500, detail="Google Sheets client not initialized")

    try:
        # Find an available account
        account = read_available_account(gspread_client)
        if not account:
            raise HTTPException(
                status_code=429, 
                detail="No available accounts found. All accounts have reached their daily limit or are not verified."
            )

        # Start scraping process
        urls = [str(request.linkedin_url)]
        batch_id = start_scraping(account, urls)
        
        if not batch_id:
            raise HTTPException(status_code=500, detail="Failed to start scraping process")

        # Wait for completion in a thread to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, wait_for_completion, batch_id)
        
        if result["status"] == "completed":
            return ScrapeResponse(
                success=True,
                message="Scraping completed successfully",
                batch_id=batch_id,
                data=result["result"][0] if result["result"] else None,
                account_used=f"Row {account['row_index']} (Usage: {account['daily_use'] + 1}/{CONFIG['daily_limit']})"
            )
        else:
            return ScrapeResponse(
                success=False,
                message=f"Scraping failed: {result.get('error', 'Unknown error')}",
                batch_id=batch_id,
                account_used=f"Row {account['row_index']} (Usage: {account['daily_use'] + 1}/{CONFIG['daily_limit']})"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in scrape-and-wait endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/status/{batch_id}", response_model=StatusResponse)
async def check_batch_status(batch_id: str):
    """
    Check the status of a scraping batch.
    """
    try:
        response = requests.get(f"{CONFIG['scrape_api']}/scrape-status/{batch_id}", timeout=30)
        response.raise_for_status()
        data = response.json()
        
        return StatusResponse(
            batch_id=batch_id,
            status=data.get("status", "unknown"),
            result=data.get("result"),
            error=data.get("error")
        )
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Status check failed for batch {batch_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to check batch status: {str(e)}")

@app.get("/accounts/status")
async def get_accounts_status():
    """
    Get the status of all LinkedIn accounts including daily usage.
    """
    if not gspread_client:
        raise HTTPException(status_code=500, detail="Google Sheets client not initialized")

    try:
        sheet = gspread_client.open(CONFIG["spreadsheet_accounts"]["name"]).worksheet(CONFIG["spreadsheet_accounts"]["sheet"])
        rows = sheet.get_all_values()
        
        if not rows or len(rows) < 2:
            return {"message": "No accounts found", "accounts": []}

        header = rows[0]
        try:
            status_idx = header.index("verification_status")
            daily_use_idx = header.index("daily_use")
            proxy_idx = header.index("Structured proxy")
        except ValueError as e:
            raise HTTPException(status_code=500, detail=f"Required column missing: {e}")

        accounts_status = []
        
        for i, row in enumerate(rows[1:], start=2):
            if len(row) <= max(status_idx, daily_use_idx, proxy_idx):
                continue
                
            status = normalize_status(row[status_idx])
            daily_use_str = row[daily_use_idx].strip()
            proxy = row[proxy_idx].strip()
            
            try:
                daily_use = int(daily_use_str) if daily_use_str else 0
            except ValueError:
                daily_use = 0
            
            accounts_status.append({
                "row": i,
                "verification_status": status,
                "daily_use": daily_use,
                "daily_limit": CONFIG["daily_limit"],
                "available": status == "verified" and daily_use < CONFIG["daily_limit"],
                "proxy": proxy[:20] + "..." if len(proxy) > 20 else proxy
            })
        
        available_count = sum(1 for acc in accounts_status if acc["available"])
        
        return {
            "total_accounts": len(accounts_status),
            "available_accounts": available_count,
            "daily_limit": CONFIG["daily_limit"],
            "accounts": accounts_status
        }
        
    except Exception as e:
        logger.error(f"Error getting accounts status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get accounts status: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)