import gspread
import requests
import time
import unicodedata
import logging
import ast
import re
import asyncio
import aiohttp
import csv
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, Optional, List
from oauth2client.service_account import ServiceAccountCredentials
import random

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
        "name": "sir",
        "sheet_urls": "Main File",
        "sheet_results": "Results"
    },
    "scrape_api": "https://linkedin-private.chitlangia.co",
    "concurrency": {
        "max_concurrent_batches": 5,
        "batch_start_delay_range": (30, 90),
        "status_check_interval": 300,
        "max_retries_per_batch": 12,
        "request_timeout": 45
    },
    "csv_output": {
        "filename": "linkedin_scraping_results_{timestamp}.csv",
        "directory": "results"
    }
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
    """Safely parses a string representation of a dictionary into a dictionary object."""
    try:
        parsed_dict = ast.literal_eval(cookie_str)
        if not isinstance(parsed_dict, dict):
            logger.warning("Parsed data is not a dictionary.")
            return {}
        
        cleaned_dict = {key: str(val).strip('"') for key, val in parsed_dict.items()}
        return cleaned_dict
    except (ValueError, SyntaxError, TypeError) as e:
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

def extract_profile_id(url: str) -> Optional[str]:
    """Extracts the profile identifier from a LinkedIn URL."""
    if not url:
        return None
    
    url = url.strip().rstrip('/')
    match = re.search(r'/in/([^/?]+)', url)
    if match:
        profile_id = match.group(1)
        logger.debug(f"Extracted profile ID '{profile_id}' from URL '{url}'")
        return profile_id
    else:
        logger.warning(f"Could not extract profile ID from URL: {url}")
        return None

def safe_string_convert(value):
    """Safely converts any value to a string and strips whitespace."""
    if value is None:
        return ''
    return str(value).strip()

def deduplicate_experiences(experiences: List[Dict]) -> List[Dict]:
    """
    Removes duplicate experience entries based on company, title, and dates.
    FIXED VERSION - Handles mixed data types properly.
    """
    if not experiences or not isinstance(experiences, list):
        return []
    
    seen = set()
    unique_experiences = []
    
    for exp in experiences:
        if not exp or not isinstance(exp, dict):
            continue
        
        # Create a unique key based on company, title, and dates
        # Use safe_string_convert to handle integers and other types
        company = safe_string_convert(exp.get('company', '')).lower()
        title = safe_string_convert(exp.get('title', '')).lower()
        start_date = safe_string_convert(exp.get('start_date', ''))
        end_date = safe_string_convert(exp.get('end_date', ''))
        
        # Create a unique identifier for this experience
        unique_key = f"{company}|{title}|{start_date}|{end_date}"
        
        if unique_key not in seen:
            seen.add(unique_key)
            unique_experiences.append(exp)
            logger.debug(f"Added unique experience: {company} - {title}")
        else:
            logger.debug(f"Skipped duplicate experience: {company} - {title}")
    
    logger.info(f"Experience deduplication: {len(experiences)} -> {len(unique_experiences)}")
    return unique_experiences

def deduplicate_education(education: List[Dict]) -> List[Dict]:
    """
    Removes duplicate education entries based on school, degree, and years.
    FIXED VERSION - Handles mixed data types properly.
    """
    if not education or not isinstance(education, list):
        return []
    
    seen = set()
    unique_education = []
    
    for edu in education:
        if not edu or not isinstance(edu, dict):
            continue
        
        # Create a unique key based on school, degree, and years
        # Use safe_string_convert to handle integers and other types
        school = safe_string_convert(edu.get('school', '')).lower()
        degree = safe_string_convert(edu.get('degree', '')).lower()
        field = safe_string_convert(edu.get('field_of_study', '')).lower()
        start_year = safe_string_convert(edu.get('start_year', ''))
        end_year = safe_string_convert(edu.get('end_year', ''))
        
        # Create a unique identifier for this education
        unique_key = f"{school}|{degree}|{field}|{start_year}|{end_year}"
        
        if unique_key not in seen:
            seen.add(unique_key)
            unique_education.append(edu)
            logger.debug(f"Added unique education: {school} - {degree}")
        else:
            logger.debug(f"Skipped duplicate education: {school} - {degree}")
    
    logger.info(f"Education deduplication: {len(education)} -> {len(unique_education)}")
    return unique_education

def flatten_profile_data(profile_data: Dict) -> Dict:
    """
    Flattens the complex nested LinkedIn profile data into a flat dictionary suitable for CSV/spreadsheet.
    FIXED VERSION - Handles different API field names and prevents duplicate entries.
    """
    flattened = {}
    
    # Extract basic profile details
    profile_details = profile_data.get('profile_details', {})
    flattened.update({
        'first_name': safe_string_convert(profile_details.get('first_name', '')),
        'last_name': safe_string_convert(profile_details.get('last_name', '')),
        'full_name': f"{safe_string_convert(profile_details.get('first_name', ''))} {safe_string_convert(profile_details.get('last_name', ''))}".strip(),
        'headline': safe_string_convert(profile_details.get('headline', '')),
        'location': safe_string_convert(profile_details.get('location', '')),
        'geo_location': safe_string_convert(profile_details.get('geo_location', '')),
        'geo_country': safe_string_convert(profile_details.get('geo_country', '')),
        'industry': safe_string_convert(profile_details.get('industry', '')),
        'public_identifier': safe_string_convert(profile_details.get('publicIdentifier', '')),
        'member_identifier': safe_string_convert(profile_details.get('memberIdentifier', '')),
        'linkedin_identifier': safe_string_convert(profile_details.get('linkedInIdentifier', '')),
        'address': safe_string_convert(profile_details.get('address', '')),
        'summary': safe_string_convert(profile_details.get('summary', '')),
        'open_to_work': profile_details.get('openToWork', False),
        'tracking_id': safe_string_convert(profile_details.get('tracking_id', '')),
    })
    
    # Add profile image
    flattened['profile_image'] = safe_string_convert(profile_data.get('image', ''))
    
    # Process experience data - FIXED VERSION WITH PROPER FIELD MAPPING
    experiences = profile_data.get('experience', [])
    logger.debug(f"Processing {len(experiences)} experience entries (before deduplication)")
    
    # Deduplicate experiences first
    unique_experiences = deduplicate_experiences_fixed(experiences)
    
    # Process only unique experiences with FIXED field mapping
    for i, exp in enumerate(unique_experiences):
        prefix = f'exp_{i}_'
        
        # Handle different possible field names for company
        company = (exp.get('company') or 
                  exp.get('companyName') or 
                  exp.get('company_name') or '')
        
        # Handle different possible field names for dates
        start_date = (exp.get('start_date') or 
                     exp.get('startDate') or 
                     exp.get('start') or '')
        
        end_date = (exp.get('end_date') or 
                   exp.get('endDate') or 
                   exp.get('end') or '')
        
        # Handle location fields
        location = (exp.get('location') or 
                   exp.get('companyLocation') or '')
        
        flattened.update({
            f'{prefix}company': safe_string_convert(company),
            f'{prefix}title': safe_string_convert(exp.get('title', '')),
            f'{prefix}description': safe_string_convert(exp.get('description', '')),
            f'{prefix}location': safe_string_convert(location),
            f'{prefix}geo_location': safe_string_convert(exp.get('geo_location', '')),
            f'{prefix}start_date': safe_string_convert(start_date),
            f'{prefix}end_date': safe_string_convert(end_date),
            f'{prefix}duration': safe_string_convert(exp.get('duration', '')),
            f'{prefix}contract_type': safe_string_convert(exp.get('contractType', '')),
            f'{prefix}company_id': safe_string_convert(exp.get('company_id', '')),
            f'{prefix}company_linked': safe_string_convert(exp.get('company_linked', '')),
            f'{prefix}company_logo': safe_string_convert(exp.get('companyLogo', '')),
            f'{prefix}linkedin_id': safe_string_convert(exp.get('linkedInId', '')),
            f'{prefix}linkedin_url': safe_string_convert(exp.get('linkedInUrl', '')),
            f'{prefix}employee_count_range': safe_string_convert(exp.get('employee_count_range', '')),
            f'{prefix}industries': ', '.join([safe_string_convert(ind) for ind in exp.get('industries', [])]) if exp.get('industries') else '',
            f'{prefix}entity_urn': safe_string_convert(exp.get('entity_urn', '')),
        })
        logger.debug(f"Added experience {i}: {safe_string_convert(company)} - {safe_string_convert(exp.get('title', 'Unknown'))}")
    
    # Process education data - ALREADY CORRECT but let's ensure robustness
    education = profile_data.get('education', [])
    logger.debug(f"Processing {len(education)} education entries (before deduplication)")
    
    # Deduplicate education first
    unique_education = deduplicate_education(education)
    
    for i, edu in enumerate(unique_education):
        prefix = f'edu_{i}_'
        flattened.update({
            f'{prefix}school': safe_string_convert(edu.get('school', '')),
            f'{prefix}degree': safe_string_convert(edu.get('degree', '')),
            f'{prefix}field_of_study': safe_string_convert(edu.get('field_of_study', '')),
            f'{prefix}start_year': safe_string_convert(edu.get('start_year', '')),
            f'{prefix}end_year': safe_string_convert(edu.get('end_year', '')),
            f'{prefix}activities': safe_string_convert(edu.get('activities', '')),
            f'{prefix}description': safe_string_convert(edu.get('description', '')),
            f'{prefix}school_id': safe_string_convert(edu.get('school_id', '')),
            f'{prefix}degree_urn': safe_string_convert(edu.get('degree_urn', '')),
            f'{prefix}field_of_study_urn': safe_string_convert(edu.get('field_of_study_urn', '')),
            f'{prefix}entity_urn': safe_string_convert(edu.get('entity_urn', '')),
        })
    
    # Process skills - FIXED VERSION WITH DEDUPLICATION
    skills = profile_data.get('skills', [])
    if skills and isinstance(skills, list):
        # Remove duplicates and empty skills
        unique_skills = []
        seen_skills = set()
        for skill in skills:
            if skill is not None:
                skill_str = safe_string_convert(skill)
                if skill_str and skill_str.lower() not in seen_skills:
                    seen_skills.add(skill_str.lower())
                    unique_skills.append(skill_str)
        
        flattened['skills_list'] = ', '.join(unique_skills)
        flattened['skills_count'] = len(unique_skills)
        
        # Add individual skills as separate columns (for first 20 unique skills)
        for i, skill in enumerate(unique_skills[:20]):
            flattened[f'skill_{i+1}'] = skill
    else:
        flattened['skills_list'] = ''
        flattened['skills_count'] = 0
    
    # Process certifications - FIXED VERSION
    certifications = profile_data.get('certifications', [])
    unique_certifications = []
    seen_certs = set()
    
    for cert in certifications:
        if not cert or not isinstance(cert, dict):
            continue
        
        # Create unique key for certification
        cert_name = safe_string_convert(cert.get('name', ''))
        cert_authority = safe_string_convert(cert.get('authority', ''))
        cert_key = f"{cert_name.lower()}|{cert_authority.lower()}"
        
        if cert_key not in seen_certs and cert_name:
            seen_certs.add(cert_key)
            unique_certifications.append(cert)
    
    for i, cert in enumerate(unique_certifications):
        prefix = f'cert_{i}_'
        flattened.update({
            f'{prefix}name': safe_string_convert(cert.get('name', '')),
            f'{prefix}authority': safe_string_convert(cert.get('authority', '')),
            f'{prefix}date': safe_string_convert(cert.get('date', '')),
            f'{prefix}url': safe_string_convert(cert.get('url', '')),
        })
    
    # Process courses - FIXED VERSION
    courses = profile_data.get('courses', [])
    unique_courses = []
    seen_courses = set()
    
    for course in courses:
        if not course or not isinstance(course, dict):
            continue
        
        # Create unique key for course
        course_name = safe_string_convert(course.get('name', ''))
        course_number = safe_string_convert(course.get('number', ''))
        course_key = f"{course_name.lower()}|{course_number}"
        
        if course_key not in seen_courses and course_name:
            seen_courses.add(course_key)
            unique_courses.append(course)
    
    for i, course in enumerate(unique_courses):
        prefix = f'course_{i}_'
        flattened.update({
            f'{prefix}name': safe_string_convert(course.get('name', '')),
            f'{prefix}number': safe_string_convert(course.get('number', '')),
        })
    
    # Process languages - FIXED VERSION
    languages = profile_data.get('languages', [])
    if languages and isinstance(languages, list):
        unique_languages = []
        seen_languages = set()
        
        for lang in languages:
            if lang and isinstance(lang, dict):
                lang_name = safe_string_convert(lang.get('name', ''))
                if lang_name and lang_name.lower() not in seen_languages:
                    seen_languages.add(lang_name.lower())
                    unique_languages.append(lang)
        
        flattened['languages_list'] = ', '.join([lang.get('name', '') for lang in unique_languages])
        flattened['languages_count'] = len(unique_languages)
        
        for i, lang in enumerate(unique_languages):
            prefix = f'lang_{i}_'
            flattened.update({
                f'{prefix}name': safe_string_convert(lang.get('name', '')),
                f'{prefix}proficiency': safe_string_convert(lang.get('proficiency', '')),
            })
    else:
        flattened['languages_list'] = ''
        flattened['languages_count'] = 0
    
    # Process honors/awards - FIXED VERSION
    honors = profile_data.get('honors', [])
    unique_honors = []
    seen_honors = set()
    
    for honor in honors:
        if not honor or not isinstance(honor, dict):
            continue
        
        # Create unique key for honor
        honor_title = safe_string_convert(honor.get('title', ''))
        honor_issuer = safe_string_convert(honor.get('issuer', ''))
        honor_key = f"{honor_title.lower()}|{honor_issuer.lower()}"
        
        if honor_key not in seen_honors and honor_title:
            seen_honors.add(honor_key)
            unique_honors.append(honor)
    
    for i, honor in enumerate(unique_honors):
        prefix = f'honor_{i}_'
        date_value = honor.get('date', '')
        if isinstance(date_value, dict):
            date_value = date_value.get('year', '')
        
        flattened.update({
            f'{prefix}title': safe_string_convert(honor.get('title', '')),
            f'{prefix}issuer': safe_string_convert(honor.get('issuer', '')),
            f'{prefix}date': safe_string_convert(date_value),
            f'{prefix}entity_urn': safe_string_convert(honor.get('entity_urn', '')),
        })
    
    # Process publications - FIXED VERSION
    publications = profile_data.get('publications', [])
    unique_publications = []
    seen_publications = set()
    
    for pub in publications:
        if not pub or not isinstance(pub, dict):
            continue
        
        # Create unique key for publication
        pub_name = safe_string_convert(pub.get('name', ''))
        pub_publisher = safe_string_convert(pub.get('publisher', ''))
        pub_key = f"{pub_name.lower()}|{pub_publisher.lower()}"
        
        if pub_key not in seen_publications and pub_name:
            seen_publications.add(pub_key)
            unique_publications.append(pub)
    
    for i, pub in enumerate(unique_publications):
        prefix = f'pub_{i}_'
        flattened.update({
            f'{prefix}name': safe_string_convert(pub.get('name', '')),
            f'{prefix}publisher': safe_string_convert(pub.get('publisher', '')),
            f'{prefix}date': safe_string_convert(pub.get('date', '')),
            f'{prefix}url': safe_string_convert(pub.get('url', '')),
            f'{prefix}description': safe_string_convert(pub.get('description', '')),
        })
    
    # Process projects - FIXED VERSION
    projects = profile_data.get('projects', [])
    unique_projects = []
    seen_projects = set()
    
    for project in projects:
        if not project or not isinstance(project, dict):
            continue
        
        # Create unique key for project
        project_name = safe_string_convert(project.get('name', ''))
        project_date = safe_string_convert(project.get('date', ''))
        project_key = f"{project_name.lower()}|{project_date}"
        
        if project_key not in seen_projects and project_name:
            seen_projects.add(project_key)
            unique_projects.append(project)
    
    for i, project in enumerate(unique_projects):
        prefix = f'project_{i}_'
        flattened.update({
            f'{prefix}name': safe_string_convert(project.get('name', '')),
            f'{prefix}description': safe_string_convert(project.get('description', '')),
            f'{prefix}url': safe_string_convert(project.get('url', '')),
            f'{prefix}date': safe_string_convert(project.get('date', '')),
        })
    
    # Process test scores - FIXED VERSION
    test_scores = profile_data.get('test_scores', [])
    unique_test_scores = []
    seen_test_scores = set()
    
    for score in test_scores:
        if not score or not isinstance(score, dict):
            continue
        
        # Create unique key for test score
        test_name = safe_string_convert(score.get('name', ''))
        test_date = safe_string_convert(score.get('date', ''))
        test_key = f"{test_name.lower()}|{test_date}"
        
        if test_key not in seen_test_scores and test_name:
            seen_test_scores.add(test_key)
            unique_test_scores.append(score)
    
    for i, score in enumerate(unique_test_scores):
        prefix = f'test_{i}_'
        flattened.update({
            f'{prefix}name': safe_string_convert(score.get('name', '')),
            f'{prefix}score': safe_string_convert(score.get('score', '')),
            f'{prefix}date': safe_string_convert(score.get('date', '')),
        })
    
    # Process volunteer experiences - FIXED VERSION
    volunteer_experiences = profile_data.get('volunteer_experiences', [])
    unique_volunteer = []
    seen_volunteer = set()
    
    for vol in volunteer_experiences:
        if not vol or not isinstance(vol, dict):
            continue
        
        # Create unique key for volunteer experience
        vol_org = safe_string_convert(vol.get('organization', ''))
        vol_role = safe_string_convert(vol.get('role', ''))
        vol_start = safe_string_convert(vol.get('start_date', ''))
        vol_key = f"{vol_org.lower()}|{vol_role.lower()}|{vol_start}"
        
        if vol_key not in seen_volunteer and vol_org:
            seen_volunteer.add(vol_key)
            unique_volunteer.append(vol)
    
    for i, vol in enumerate(unique_volunteer):
        prefix = f'volunteer_{i}_'
        flattened.update({
            f'{prefix}organization': safe_string_convert(vol.get('organization', '')),
            f'{prefix}role': safe_string_convert(vol.get('role', '')),
            f'{prefix}cause': safe_string_convert(vol.get('cause', '')),
            f'{prefix}description': safe_string_convert(vol.get('description', '')),
            f'{prefix}start_date': safe_string_convert(vol.get('start_date', '')),
            f'{prefix}end_date': safe_string_convert(vol.get('end_date', '')),
            f'{prefix}entity_urn': safe_string_convert(vol.get('entity_urn', '')),
        })
    
    logger.debug(f"Flattened profile data contains {len(flattened)} fields")
    return flattened


def deduplicate_experiences_fixed(experiences: List[Dict]) -> List[Dict]:
    """
    Removes duplicate experience entries based on company, title, and dates.
    FIXED VERSION - Handles different API field names and mixed data types properly.
    """
    if not experiences or not isinstance(experiences, list):
        return []
    
    seen = set()
    unique_experiences = []
    
    for exp in experiences:
        if not exp or not isinstance(exp, dict):
            continue
        
        # Handle different possible field names for company
        company = safe_string_convert(
            exp.get('company') or 
            exp.get('companyName') or 
            exp.get('company_name') or ''
        ).lower()
        
        # Handle different possible field names for dates
        start_date = safe_string_convert(
            exp.get('start_date') or 
            exp.get('startDate') or 
            exp.get('start') or ''
        )
        
        end_date = safe_string_convert(
            exp.get('end_date') or 
            exp.get('endDate') or 
            exp.get('end') or ''
        )
        
        title = safe_string_convert(exp.get('title', '')).lower()
        
        # Create a unique identifier for this experience
        unique_key = f"{company}|{title}|{start_date}|{end_date}"
        
        if unique_key not in seen:
            seen.add(unique_key)
            unique_experiences.append(exp)
            logger.debug(f"Added unique experience: {company} - {title}")
        else:
            logger.debug(f"Skipped duplicate experience: {company} - {title}")
    
    logger.info(f"Experience deduplication: {len(experiences)} -> {len(unique_experiences)}")
    return unique_experiences

def process_api_response(api_response: Dict) -> List[Dict]:
    """
    Processes the API response and converts it to a list of flattened profile dictionaries.
    Handles the batch response structure with status and result fields.
    """
    processed_profiles = []
    
    # Check if this is a batch response
    if 'result' in api_response and isinstance(api_response['result'], list):
        profiles = api_response['result']
    elif isinstance(api_response, list):
        profiles = api_response
    else:
        # Single profile response
        profiles = [api_response]
    
    for profile in profiles:
        try:
            flattened_profile = flatten_profile_data(profile)
            processed_profiles.append(flattened_profile)
            logger.debug(f"Successfully processed profile: {flattened_profile.get('full_name', 'Unknown')}")
        except Exception as e:
            logger.error(f"Error processing profile: {e}")
            logger.debug(f"Problematic profile data: {profile}")
            continue
    
    logger.info(f"Successfully processed {len(processed_profiles)} profiles from API response")
    return processed_profiles

def write_results_to_csv(results: List[Dict]) -> str:
    """Writes the collected data to a CSV file with timestamp - IMPROVED VERSION."""
    if not results:
        logger.warning("No results to write to CSV.")
        return None
    
    # Create results directory if it doesn't exist
    results_dir = CONFIG["csv_output"]["directory"]
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = CONFIG["csv_output"]["filename"].format(timestamp=timestamp)
    filepath = os.path.join(results_dir, filename)
    
    try:
        # Get all unique keys from all results
        all_keys = set()
        for result in results:
            all_keys.update(result.keys())
        
        # Define preferred column order for better readability
        preferred_order = [
            # Basic Profile Info
            'full_name', 'first_name', 'last_name', 'headline', 'location', 'geo_location', 'geo_country',
            'industry', 'public_identifier', 'member_identifier', 'linkedin_identifier',
            
            # Contact & Demographics
            'address', 'summary', 'open_to_work', 'profile_image',
            
            # Skills Summary
            'skills_count', 'skills_list',
            
            # Languages Summary
            'languages_count', 'languages_list',
        ]
        
        # Group remaining fields by category with improved sorting
        def extract_index_from_field(field_name):
            """Extract numeric index from field names like exp_0_title, exp_10_company"""
            parts = field_name.split('_')
            if len(parts) >= 3:
                try:
                    return int(parts[1])
                except ValueError:
                    pass
            return 999  # Put non-indexed fields at the end
        
        def sort_indexed_fields(fields):
            """Sort fields like exp_0_title, exp_1_title, exp_10_title correctly."""
            return sorted(fields, key=lambda x: (extract_index_from_field(x), x))
        
        # Categorize and sort fields
        experience_fields = sort_indexed_fields([f for f in all_keys if f.startswith('exp_')])
        education_fields = sort_indexed_fields([f for f in all_keys if f.startswith('edu_')])
        skill_fields = sorted([f for f in all_keys if f.startswith('skill_') and f not in ['skills_list', 'skills_count']])
        certification_fields = sort_indexed_fields([f for f in all_keys if f.startswith('cert_')])
        language_fields = sort_indexed_fields([f for f in all_keys if f.startswith('lang_') and f not in ['languages_list', 'languages_count']])
        volunteer_fields = sort_indexed_fields([f for f in all_keys if f.startswith('volunteer_')])
        honor_fields = sort_indexed_fields([f for f in all_keys if f.startswith('honor_')])
        publication_fields = sort_indexed_fields([f for f in all_keys if f.startswith('pub_')])
        project_fields = sort_indexed_fields([f for f in all_keys if f.startswith('project_')])
        test_fields = sort_indexed_fields([f for f in all_keys if f.startswith('test_')])
        course_fields = sort_indexed_fields([f for f in all_keys if f.startswith('course_')])
        
        # Get remaining fields
        categorized_fields = set(
            experience_fields + education_fields + skill_fields + certification_fields +
            language_fields + volunteer_fields + honor_fields + publication_fields +
            project_fields + test_fields + course_fields
        )
        other_fields = sorted([f for f in all_keys if f not in categorized_fields and f not in preferred_order])
        
        # Build final field order
        fieldnames = []
        
        # Add preferred fields that exist in data
        for field in preferred_order:
            if field in all_keys:
                fieldnames.append(field)
        
        # Add categorized fields in logical order
        fieldnames.extend(experience_fields)
        fieldnames.extend(education_fields)
        fieldnames.extend(skill_fields)
        fieldnames.extend(certification_fields)
        fieldnames.extend(language_fields)
        fieldnames.extend(volunteer_fields)
        fieldnames.extend(honor_fields)
        fieldnames.extend(publication_fields)
        fieldnames.extend(project_fields)
        fieldnames.extend(course_fields)
        fieldnames.extend(test_fields)
        fieldnames.extend(other_fields)
        
        # Write to CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for result in results:
                # Ensure all fields are present, fill missing with empty string
                row = {field: result.get(field, '') for field in fieldnames}
                writer.writerow(row)
        
        # Log summary
        logger.info(f"Successfully wrote {len(results)} results to CSV file: {filepath}")
        logger.info(f"CSV contains {len(fieldnames)} columns organized as follows:")
        logger.info(f"  - Basic info: {len([f for f in preferred_order if f in all_keys])} columns")
        logger.info(f"  - Experience: {len(experience_fields)} columns")
        logger.info(f"  - Education: {len(education_fields)} columns")
        logger.info(f"  - Skills: {len(skill_fields)} columns")
        logger.info(f"  - Certifications: {len(certification_fields)} columns")
        logger.info(f"  - Languages: {len(language_fields)} columns")
        logger.info(f"  - Volunteer: {len(volunteer_fields)} columns")
        logger.info(f"  - Honors: {len(honor_fields)} columns")
        logger.info(f"  - Publications: {len(publication_fields)} columns")
        logger.info(f"  - Projects: {len(project_fields)} columns")
        logger.info(f"  - Courses: {len(course_fields)} columns")
        logger.info(f"  - Tests: {len(test_fields)} columns")
        logger.info(f"  - Other: {len(other_fields)} columns")
        
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to write results to CSV file: {e}")
        return None

def write_results(client: gspread.Client, results: List[Dict]):
    """Writes the collected data to the 'Results' sheet."""
    if not results:
        logger.warning("No results to write.")
        return
        
    if not client:
        return

    try:
        sheet = client.open(CONFIG["spreadsheet_urls"]["name"]).worksheet(CONFIG["spreadsheet_urls"]["sheet_results"])
        
        if results:
            headers = list(results[0].keys())
            # Ensure all rows have the same keys in the same order
            values = [headers] + [[row.get(h, "") for h in headers] for row in results]

            sheet.clear()
            sheet.update("A1", values, value_input_option='USER_ENTERED')
            logger.info(f"Successfully wrote {len(results)} results to the '{CONFIG['spreadsheet_urls']['sheet_results']}' sheet.")
        
    except Exception as e:
        logger.error(f"Failed to write results to Google Sheet: {e}")

def read_accounts(client: gspread.Client) -> List[Dict]:
    """Reads verified accounts from the 'LinkedIn Accounts' spreadsheet."""
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
        structured_proxy_idx = header.index("Structured proxy")
    except ValueError as e:
        logger.error(f"A required column is missing from the 'LinkedIn Accounts' sheet: {e}")
        return []

    accounts = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) <= max(session_idx, structured_proxy_idx, status_idx):
            logger.warning(f"Row {i} is incomplete and will be skipped.")
            continue

        status = normalize_status(row[status_idx])
        if status != "verified":
            logger.info(f"Row {i} skipped: Account status is '{status}', not 'verified'.")
            continue

        session_str = row[session_idx].strip()
        proxy = row[structured_proxy_idx].strip()

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
    """Reads profile URLs from the 'Private Data' sheet and converts them to profile identifiers."""
    if not client:
        return []
        
    try:
        sheet = client.open(CONFIG["spreadsheet_urls"]["name"]).worksheet(CONFIG["spreadsheet_urls"]["sheet_urls"])
        rows = sheet.get_all_values()
    except Exception as e:
        logger.error(f"Failed to read profile URLs: {e}")
        return []

    if not rows or len(rows) < 2:
        logger.warning("No URLs found in 'Private Data' sheet.")
        return []

    try:
        url_idx = rows[0].index("Linkedin Url")
    except ValueError:
        logger.error("'Linkedin Url' column not found.")
        return []

    profile_ids = []
    for i, row in enumerate(rows[1:], start=2):
        if len(row) > url_idx and row[url_idx].strip():
            original_url = row[url_idx].strip()
            profile_id = extract_profile_id(original_url)
            
            if profile_id:
                profile_ids.append(profile_id)
                logger.debug(f"Row {i}: Converted '{original_url}' to '{profile_id}'")
            else:
                logger.warning(f"Row {i}: Could not extract profile ID from '{original_url}', skipping.")
    
    logger.info(f"Successfully converted {len(profile_ids)} URLs to profile identifiers.")
    return profile_ids

def split_batches(data: list, size: int) -> list:
    """Splits a list into smaller chunks of a specified size."""
    return [data[i:i + size] for i in range(0, len(data), size)]

async def start_scraping_async(session: aiohttp.ClientSession, account: Dict, profile_ids: List[str], batch_number: int) -> Optional[str]:
    """Initiates a scraping job via the API using profile identifiers (async version)."""
    jsessionid = account["JSESSIONID"].replace("ajax:", "")

    payload = {
        "JSESSIONID": jsessionid,
        "li_at": account["li_at"],
        "profile_urls": profile_ids,
        "proxy": account["proxy"]
    }

    try:
        logger.info(f"Starting batch {batch_number} with {len(profile_ids)} profiles using proxy {account['proxy']}")
        
        async with session.post(
            f"{CONFIG['scrape_api']}/scrape-linkedin", 
            json=payload, 
            timeout=aiohttp.ClientTimeout(total=CONFIG["concurrency"]["request_timeout"])
        ) as response:
            response.raise_for_status()
            data = await response.json()
            batch_id = data.get("batch_id")
            logger.info(f"Batch {batch_number} started successfully with batch ID: {batch_id}")
            return batch_id
            
    except Exception as e:
        logger.error(f"Failed to start batch {batch_number}: {e}")
        return None

async def check_batch_status_async(session: aiohttp.ClientSession, batch_id: str, batch_number: int) -> List[Dict]:
    """Checks the status of a batch asynchronously."""
    try:
        async with session.get(
            f"{CONFIG['scrape_api']}/scrape-status/{batch_id}",
            timeout=aiohttp.ClientTimeout(total=CONFIG["concurrency"]["request_timeout"])
        ) as response:
            response.raise_for_status()
            data = await response.json()
            
            status = data.get("status")
            if status == "completed":
                logger.info(f"Batch {batch_number} ({batch_id}) completed successfully.")
                # Process the API response to flatten the data
                return process_api_response(data)
            elif status == "failed":
                logger.error(f"Batch {batch_number} ({batch_id}) failed. Reason: {data.get('error')}")
                return []
            else:
                # Still in progress
                return None
                
    except Exception as e:
        logger.error(f"Status check failed for batch {batch_number} ({batch_id}): {e}")
        return []

async def wait_for_batch_completion(session: aiohttp.ClientSession, batch_id: str, batch_number: int) -> List[Dict]:
    """Waits for a batch to complete with exponential backoff."""
    max_retries = CONFIG["concurrency"]["max_retries_per_batch"]
    base_interval = CONFIG["concurrency"]["status_check_interval"]
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Checking batch {batch_number} ({batch_id}) - Attempt {attempt + 1}/{max_retries}")
            
            result = await check_batch_status_async(session, batch_id, batch_number)
            
            if result is not None:  # Either completed successfully or failed
                return result
            
            # Still in progress - wait with some jitter
            jitter = random.uniform(0.8, 1.2)
            wait_time = base_interval * jitter
            
            logger.info(f"Batch {batch_number} still in progress. Waiting {wait_time:.1f} seconds...")
            await asyncio.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Error checking batch {batch_number}: {e}")
            await asyncio.sleep(base_interval)
    
    logger.warning(f"Batch {batch_number} ({batch_id}) did not complete after {max_retries} attempts.")
    return []

async def process_batch(session: aiohttp.ClientSession, account: Dict, profile_ids: List[str], batch_number: int, semaphore: asyncio.Semaphore) -> List[Dict]:
    """Processes a single batch with concurrency control."""
    async with semaphore:
        # Add random delay before starting each batch
        delay = random.uniform(*CONFIG["concurrency"]["batch_start_delay_range"])
        logger.info(f"Batch {batch_number} waiting {delay:.1f} seconds before starting...")
        await asyncio.sleep(delay)
        
        # Start the scraping job
        batch_id = await start_scraping_async(session, account, profile_ids, batch_number)
        
        if not batch_id:
            logger.error(f"Failed to start batch {batch_number}")
            return []
        
        # Wait for completion
        return await wait_for_batch_completion(session, batch_id, batch_number)

async def process_all_batches_concurrent(accounts: List[Dict], batches: List[List[str]]) -> List[Dict]:
    """Processes all batches concurrently with controlled concurrency."""
    num_to_process = min(len(accounts), len(batches))
    logger.info(f"Starting concurrent processing of {num_to_process} batches")
    logger.info(f"Concurrency settings: max_concurrent={CONFIG['concurrency']['max_concurrent_batches']}, "
               f"start_delay={CONFIG['concurrency']['batch_start_delay_range']}s")
    
    # Create semaphore to limit concurrent batches
    semaphore = asyncio.Semaphore(CONFIG["concurrency"]["max_concurrent_batches"])
    
    # Create HTTP session with connection pooling
    connector = aiohttp.TCPConnector(
        limit=20,  # Total connection pool size
        limit_per_host=5,  # Max connections per host
        ttl_dns_cache=300,  # DNS cache TTL
        use_dns_cache=True,
    )
    
    timeout = aiohttp.ClientTimeout(total=CONFIG["concurrency"]["request_timeout"])
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Create tasks for all batches
        tasks = []
        for i in range(num_to_process):
            account = accounts[i]
            batch_profile_ids = batches[i]
            
            task = process_batch(session, account, batch_profile_ids, i + 1, semaphore)
            tasks.append(task)
        
        # Wait for all batches to complete
        logger.info("All batches started. Waiting for completion...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        all_results = []
        successful_batches = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Batch {i + 1} raised an exception: {result}")
            elif isinstance(result, list) and result:
                all_results.extend(result)
                successful_batches += 1
                logger.info(f"Batch {i + 1} completed with {len(result)} results")
            else:
                logger.warning(f"Batch {i + 1} completed with no results")
        
        logger.info(f"Concurrent processing completed: {successful_batches}/{num_to_process} batches successful")
        logger.info(f"Total results collected: {len(all_results)}")
        
        return all_results

def run_concurrent_scraping(accounts: List[Dict], profile_ids: List[str]) -> List[Dict]:
    """Main function to run concurrent scraping with proper event loop handling."""
    # Split profile IDs into batches of 50
    batches = split_batches(profile_ids, 50)
    
    # Run the async processing
    try:
        # Check if we're already in an event loop (e.g., in Jupyter)
        loop = asyncio.get_running_loop()
        logger.warning("Already in an event loop. Using nest_asyncio might be needed.")
        # If we're in a running loop, we need to handle this differently
        import nest_asyncio
        nest_asyncio.apply()
        return asyncio.run(process_all_batches_concurrent(accounts, batches))
    except RuntimeError:
        # No event loop running, safe to use asyncio.run
        return asyncio.run(process_all_batches_concurrent(accounts, batches))

# === MAIN EXECUTION ===
if __name__ == "__main__":
    logger.info("--- Starting LinkedIn Scraping Process (Concurrent Mode) ---")

    # Initialize the client once and pass it to functions
    gspread_client = get_gsheet_client()
    if not gspread_client:
        logger.error("Execution stopped: Could not authorize with Google Sheets.")
        exit()

    accounts = read_accounts(gspread_client)
    profile_ids = read_profile_urls(gspread_client)

    if not accounts:
        logger.error("Execution stopped: No verified accounts were found.")
        exit()
    
    if not profile_ids:
        logger.error("Execution stopped: No profile identifiers were found to process.")
        exit()

    logger.info(f"Loaded {len(accounts)} accounts and {len(profile_ids)} profile IDs")
    
    # Run concurrent scraping
    all_results = run_concurrent_scraping(accounts, profile_ids)

    # Save results to CSV and Google Sheets
    if all_results:
        # Write to CSV file (NEW)
        csv_filepath = write_results_to_csv(all_results)
        if csv_filepath:
            logger.info(f"✅ All scraped data saved to: {csv_filepath}")
        
        # Also write to Google Sheets (original functionality)
        write_results(gspread_client, all_results)
        logger.info(f"Successfully completed scraping with {len(all_results)} total results")
    else:
        logger.warning("No data was collected from any batch.")