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


# def proxy_format(proxy_string):
#     try:
#         host, port, username, password = proxy_string.split(":")
#         return f"http://{username}:{password}@{host}:{port}"
#     except ValueError:
#         raise ValueError("Invalid proxy string format. Expected format: host:port:username:password")

def process_profiles(batch_id,user_agent, jsessionid, li_at, profile_urls, proxy=None):
    try:
        # Set up headers for LinkedIn API

        headers = {
            'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'accept-language': 'en-US,en;q=0.9',
            'csrf-token': f'ajax:{jsessionid}',
            'priority': 'u=1, i',
            'user-agent': user_agent,
            # 'x-li-track': '{"clientVersion":"1.13.36147","mpVersion":"1.13.36147","osName":"web","timezoneOffset":-5,"timezone":"America/Chicago","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":2,"displayWidth":2940,"displayHeight":1912}',
            'x-restli-protocol-version': '2.0.0',
            # 'cookie': 'bcookie="v=2&73224489-5d3a-4ef1-8484-1fa0197e4b00"; bscookie="v=1&20250214121209b47d986c-1bee-4064-821d-0874f2698a7dAQFL-Tf0UsTTgTqNBBQoU8igWfWtkHT7"; li_rm=AQFa3Nun3KiwHgAAAZUEZDP9fw3BG1qyGeet_1JJ2QHBRMmHrOHODz-t_uoCjQO0O4BNPA4jgM4zs9bjo9GjsYF-5VU-Bzp5YtSsywwgrLcc-r1ihdeF0mVQkMnW1uIK8rf5Ty-sg3R8X39HO2jqnuIa3CQkKvCLUlDgVK2QkYfrcbsmIt_uhF40O6vHM6-zwhu5NNgbuDWLvtlUPTaWM-bxck7QjrdZ381AjIunAuJF_UlFTeN7wiBDmVBjzHTouYtQJiSlB0gU4GLYQ8MhKIaTXAGk1oK--oLMEULJEJh0eU4XjSVH4cNtElPQiuGhviEu_G3zeZ52RZCPrjg; JSESSIONID="ajax:5407826422185488693"; li_theme=light; li_theme_set=app; li_sugr=76a61826-9c4f-4ad1-b4b9-4039569c8c10; dfpfpt=79fa1693f80f42499405ca8de1406c1e; lang=v=2&lang=en-us; li_at=AQEDATEFsqEF7K_tAAABl1TfUbYAAAGXeOvVtk0ARU5xZLOVZkGfAuRAj4ANklkZFHjjDHa0JoRciKY9r9ZSbcN80RMdKsqb0KMAKYxo58HbJlHYVh_4gMh_8XwaZrQbt8gYEH-4Ayf-RGJB9GR-94Z2; liap=true; timezone=America/Chicago; UserMatchHistory=AQLRxwUYIf5F6AAAAZdU34VV-dXse2FMU3XyceEqRKctW-rCf2tBeHJDFZjNrW01nynw1g9RSlKcfaozo1nXak98p-t8yClxMdMeiZ5bwZL9gL9o64iBAwKE__EmIUErdZbHCx1qOVl5E78_CcUGiTii0OdlLXXEIJmp86ap6YlR6kZJ0StQHgBM_tqO_ni_b4EscmngInykRtPzaj6RaW7pPKiowg6XR4z0sAmWwDmYHDEe1H66gkUMBu6981FnH2yLriWdIEWR7aF5-B2fQbp0re0qlY3G4fz01DRzkyLj8UYLpJVjAOdICBm9tTqlLbmM8F2acgAa51D10_xUTqnlfHHf4PWMf-IB_d0vi4L09gqIww; AnalyticsSyncHistory=AQI2uSWRp7MpCAAAAZdU34VVKlZR2zzXh0NbG_llQ4RkjqqDfEVlMGpzyC3bxb-18SmP2PjmSOjEoXgADQvg4w; lms_ads=AQGS-EavGybxZgAAAZdU34ZFfjnBHOcg7K_UOjmszgJNloBGPB4ehfkzRi2yg7wtLKoYYjo5r518ranzttbp66UsV84zc0fC; lms_analytics=AQGS-EavGybxZgAAAZdU34ZFfjnBHOcg7K_UOjmszgJNloBGPB4ehfkzRi2yg7wtLKoYYjo5r518ranzttbp66UsV84zc0fC; _guid=e91f06f3-6ae0-4870-921b-f5e265e593bd; lidc="b=VB93:s=V:r=V:a=V:p=V:g=4191:u=571:x=1:i=1749475630:t=1749499751:v=2:sig=AQGDLz1rnrDNUcwXMWDyRrRP-VTVUunm"; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C20249%7CMCMID%7C23216564369143702163963164319688294988%7CMCAAMLH-1750080431%7C7%7CMCAAMB-1750080431%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1749482831s%7CNONE%7CvVersion%7C5.1.1%7CMCCIDH%7C1476003076; aam_uuid=23753684545514550753904853766321795463; _gcl_au=1.1.517968955.1749475634; fptctx2=taBcrIH61PuCVH7eNCyH0Iitb%252bEMfwlgK%252fM8w%252f28EbfzJKkBisHlSHSWXKdZciWrcg6m5N55K%252bEhloH7u%252fjFFvpRsDb2RA9UxCnSpqup1ldhL1Ax%252fMvrSNDkDW3rDLkR73fsYdBZ%252b91eanptkzPO4vElAzHyfgx%252fAQu919glw9ojnPp7JVgSJJ0rh1F7Hav5dakKQJ5NM4St5rZyrh8wmQY3pqi8RlvFpRRQZoJDPaTDjm5ZBchdpr74K%252biDqwsKpOwNXAnj%252ffIZ7MnJbdHN4i1gP404OK6s9fjQYJsUlFU2oMmwL3dwYsh6%252fhbjIZKIWM6oI9y1w3g2o9UE9BUmat6JlxIGaDrSbjqL1K5QkwE%253d; __cf_bm=t_HIZk4REVNjtgixRmW8ebQh2ZH3AFYKa2gouVoX3qQ-1749476509-1.0.1.1-zQUAghOhT6f87r4EiTiSwXbUzMcAKK.zYD2BwOWBnl8LIVL5VKGp5QVWPO5CYzKZ0LkmdEYB5qYJ_AX.uI1XFgcwFe12yH3STyzPTyf6s_4',
            'cookie': f'JSESSIONID="ajax:{jsessionid}";li_at={li_at}',
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

def process_profiles_simple(batch_id,user_agent, jsessionid, li_at, profile_urls, proxy=None):
    try:
        # Set up headers for LinkedIn API

        headers = {
            'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'accept-language': 'en-US,en;q=0.9',
            'csrf-token': f'ajax:{jsessionid}',
            'priority': 'u=1, i',
            'user-agent': user_agent,
            # 'x-li-track': '{"clientVersion":"1.13.36147","mpVersion":"1.13.36147","osName":"web","timezoneOffset":-5,"timezone":"America/Chicago","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":2,"displayWidth":2940,"displayHeight":1912}',
            'x-restli-protocol-version': '2.0.0',
            # 'cookie': 'bcookie="v=2&73224489-5d3a-4ef1-8484-1fa0197e4b00"; bscookie="v=1&20250214121209b47d986c-1bee-4064-821d-0874f2698a7dAQFL-Tf0UsTTgTqNBBQoU8igWfWtkHT7"; li_rm=AQFa3Nun3KiwHgAAAZUEZDP9fw3BG1qyGeet_1JJ2QHBRMmHrOHODz-t_uoCjQO0O4BNPA4jgM4zs9bjo9GjsYF-5VU-Bzp5YtSsywwgrLcc-r1ihdeF0mVQkMnW1uIK8rf5Ty-sg3R8X39HO2jqnuIa3CQkKvCLUlDgVK2QkYfrcbsmIt_uhF40O6vHM6-zwhu5NNgbuDWLvtlUPTaWM-bxck7QjrdZ381AjIunAuJF_UlFTeN7wiBDmVBjzHTouYtQJiSlB0gU4GLYQ8MhKIaTXAGk1oK--oLMEULJEJh0eU4XjSVH4cNtElPQiuGhviEu_G3zeZ52RZCPrjg; JSESSIONID="ajax:5407826422185488693"; li_theme=light; li_theme_set=app; li_sugr=76a61826-9c4f-4ad1-b4b9-4039569c8c10; dfpfpt=79fa1693f80f42499405ca8de1406c1e; lang=v=2&lang=en-us; li_at=AQEDATEFsqEF7K_tAAABl1TfUbYAAAGXeOvVtk0ARU5xZLOVZkGfAuRAj4ANklkZFHjjDHa0JoRciKY9r9ZSbcN80RMdKsqb0KMAKYxo58HbJlHYVh_4gMh_8XwaZrQbt8gYEH-4Ayf-RGJB9GR-94Z2; liap=true; timezone=America/Chicago; UserMatchHistory=AQLRxwUYIf5F6AAAAZdU34VV-dXse2FMU3XyceEqRKctW-rCf2tBeHJDFZjNrW01nynw1g9RSlKcfaozo1nXak98p-t8yClxMdMeiZ5bwZL9gL9o64iBAwKE__EmIUErdZbHCx1qOVl5E78_CcUGiTii0OdlLXXEIJmp86ap6YlR6kZJ0StQHgBM_tqO_ni_b4EscmngInykRtPzaj6RaW7pPKiowg6XR4z0sAmWwDmYHDEe1H66gkUMBu6981FnH2yLriWdIEWR7aF5-B2fQbp0re0qlY3G4fz01DRzkyLj8UYLpJVjAOdICBm9tTqlLbmM8F2acgAa51D10_xUTqnlfHHf4PWMf-IB_d0vi4L09gqIww; AnalyticsSyncHistory=AQI2uSWRp7MpCAAAAZdU34VVKlZR2zzXh0NbG_llQ4RkjqqDfEVlMGpzyC3bxb-18SmP2PjmSOjEoXgADQvg4w; lms_ads=AQGS-EavGybxZgAAAZdU34ZFfjnBHOcg7K_UOjmszgJNloBGPB4ehfkzRi2yg7wtLKoYYjo5r518ranzttbp66UsV84zc0fC; lms_analytics=AQGS-EavGybxZgAAAZdU34ZFfjnBHOcg7K_UOjmszgJNloBGPB4ehfkzRi2yg7wtLKoYYjo5r518ranzttbp66UsV84zc0fC; _guid=e91f06f3-6ae0-4870-921b-f5e265e593bd; lidc="b=VB93:s=V:r=V:a=V:p=V:g=4191:u=571:x=1:i=1749475630:t=1749499751:v=2:sig=AQGDLz1rnrDNUcwXMWDyRrRP-VTVUunm"; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C20249%7CMCMID%7C23216564369143702163963164319688294988%7CMCAAMLH-1750080431%7C7%7CMCAAMB-1750080431%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1749482831s%7CNONE%7CvVersion%7C5.1.1%7CMCCIDH%7C1476003076; aam_uuid=23753684545514550753904853766321795463; _gcl_au=1.1.517968955.1749475634; fptctx2=taBcrIH61PuCVH7eNCyH0Iitb%252bEMfwlgK%252fM8w%252f28EbfzJKkBisHlSHSWXKdZciWrcg6m5N55K%252bEhloH7u%252fjFFvpRsDb2RA9UxCnSpqup1ldhL1Ax%252fMvrSNDkDW3rDLkR73fsYdBZ%252b91eanptkzPO4vElAzHyfgx%252fAQu919glw9ojnPp7JVgSJJ0rh1F7Hav5dakKQJ5NM4St5rZyrh8wmQY3pqi8RlvFpRRQZoJDPaTDjm5ZBchdpr74K%252biDqwsKpOwNXAnj%252ffIZ7MnJbdHN4i1gP404OK6s9fjQYJsUlFU2oMmwL3dwYsh6%252fhbjIZKIWM6oI9y1w3g2o9UE9BUmat6JlxIGaDrSbjqL1K5QkwE%253d; __cf_bm=t_HIZk4REVNjtgixRmW8ebQh2ZH3AFYKa2gouVoX3qQ-1749476509-1.0.1.1-zQUAghOhT6f87r4EiTiSwXbUzMcAKK.zYD2BwOWBnl8LIVL5VKGp5QVWPO5CYzKZ0LkmdEYB5qYJ_AX.uI1XFgcwFe12yH3STyzPTyf6s_4',
            'cookie': f'JSESSIONID="ajax:{jsessionid}";li_at={li_at}',
        }
        # Initialize LinkedIn API client with proxy string

        api = Linkedin(headers=headers, proxy=proxy)
        all_profiles = []

        # Process each profile URL
        for username in profile_urls:
            try:
                time.sleep(3)  # Avoid rate limiting
                profile = api.get_profile_without(username)
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
        user_agent=data.get('user_agent')
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
            args=(batch_id,user_agent,jsessionid, li_at, profile_urls, proxy),
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

@app.route('/scrape-linkedin_simple', methods=['POST'])
def scrape_linkedin():
    try:
        # Get JSON data from request
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # Extract JSESSIONID, li_at, profile_urls, and proxy
        jsessionid = data.get('JSESSIONID')
        li_at = data.get('li_at')
        user_agent=data.get('user_agent')
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
            target=process_profiles_simple,
            args=(batch_id,user_agent,jsessionid, li_at, profile_urls, proxy),
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

