from flask import Flask, request, jsonify
import json
import pandas as pd
import time
import threading
import uuid
import logging
from linkedin_helper import Linkedin

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory storage for batch status and results
batch_jobs = {}


def process_profiles(batch_id, jsessionid, li_at, profile_urls, proxy=None):
    try:
        # Set up headers for LinkedIn API
        headers = {
            'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'accept-language': 'en-US,en;q=0.9',
            'csrf-token': f'ajax:{jsessionid}',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'cookie': f'JSESSIONID="{jsessionid}";li_at={li_at}',
        }

        # Initialize LinkedIn API client with proxy string
        api = Linkedin(headers=headers, proxy=proxy)
        all_profiles = []

        # Process each profile URL
        for username in profile_urls:
            try:
                time.sleep(3)  # Avoid rate limiting
                profile = api.get_profile(username)
                df = api.flatten_linkedin_profile(profile)
                all_profiles.append(df)
                logger.info(f"Done processing profile: {username}")
            except Exception as e:
                logger.error(f"Error fetching {username}: {e}")
                continue

        # Update batch status and results
        if all_profiles:
            combined_df = pd.concat(all_profiles, ignore_index=True)
            batch_jobs[batch_id] = {
                'status': 'completed',
                'result': combined_df.to_dict(orient='records')
            }
        else:
            batch_jobs[batch_id] = {
                'status': 'failed',
                'error': 'No profiles were successfully processed'
            }
    except Exception as e:
        logger.error(f"Error in batch {batch_id}: {e}")
        batch_jobs[batch_id] = {
            'status': 'failed',
            'error': str(e)
        }


@app.route('/scrape-linkedin', methods=['POST'])
def scrape_linkedin():
    try:
        # Get JSON data from request
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # Extract JSESSIONID, li_at, profile_urls, and proxy
        jsessionid = data.get('JSESSIONID')
        li_at = data.get('li_at')
        profile_urls = data.get('profile_urls')
        proxy = data.get('proxy')  # Proxy string in format host:port:username:password

        # Validate inputs
        if not jsessionid or not li_at or not profile_urls:
            return jsonify({"error": "Missing JSESSIONID, li_at, or profile_urls"}), 400
        if not isinstance(profile_urls, list):
            return jsonify({"error": "profile_urls must be a list"}), 400
        if proxy:
            if not isinstance(proxy, str):
                return jsonify({"error": "proxy must be a string"}), 400
            # Validate proxy format (host:port:username:password)
            parts = proxy.split(':')
            if len(parts) != 4:
                return jsonify({"error": "proxy must be in format 'host:port:username:password'"}), 400

        # Generate unique batch ID
        batch_id = str(uuid.uuid4())

        # Initialize batch job
        batch_jobs[batch_id] = {'status': 'in_progress'}

        # Start background processing
        threading.Thread(
            target=process_profiles,
            args=(batch_id, jsessionid, li_at, profile_urls, proxy),
            daemon=True
        ).start()

        # Return batch ID and status
        return jsonify({
            'batch_id': batch_id,
            'status': 'in_progress',
            'message': 'Profile scraping started. Check status with batch_id.'
        }), 202

    except Exception as e:
        logger.error(f"Error in scrape_linkedin: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/scrape-status/<batch_id>', methods=['GET'])
def check_status(batch_id):
    try:
        # Check if batch_id exists
        if batch_id not in batch_jobs:
            return jsonify({"error": "Invalid batch_id"}), 404

        job = batch_jobs[batch_id]
        if job['status'] == 'in_progress':
            return jsonify({
                'batch_id': batch_id,
                'status': 'in_progress',
                'message': 'Profile scraping is still in progress.'
            }), 200
        elif job['status'] == 'completed':
            return jsonify({
                'batch_id': batch_id,
                'status': 'completed',
                'result': job['result']
            }), 200
        else:  # failed
            return jsonify({
                'batch_id': batch_id,
                'status': 'failed',
                'error': job['error']
            }), 400

    except Exception as e:
        logger.error(f"Error checking status for batch {batch_id}: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

