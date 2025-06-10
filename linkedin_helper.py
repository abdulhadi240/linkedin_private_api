"""
Provides linkedin api-related code
"""

import json
import logging
import random
import time
import uuid
import re
import pandas as pd
from operator import itemgetter
from time import sleep
from urllib.parse import urlencode, quote
from typing import Dict, Union, Optional, List, Literal

from linkedin_api.client import Client
from linkedin_api.utils.helpers import (
    get_id_from_urn,
    get_urn_from_raw_update,
    get_list_posts_sorted_without_promoted,
    parse_list_raw_posts,
    parse_list_raw_urns,
    generate_trackingId,
    generate_trackingId_as_charString,
)

logger = logging.getLogger(__name__)


def default_evade():
    """
    A catch-all method to try and evade suspension from Linkedin.
    Currenly, just delays the request by a random (bounded) time
    """
    sleep(random.randint(2, 5))  # sleep a random duration to try and evade suspention


class Linkedin(object):
    """
    Class for accessing the LinkedIn API.

    :param username: Username of LinkedIn account.
    :type username: str
    :param password: Password of LinkedIn account.
    :type password: str
    :param headers: Custom headers for requests (bypasses authentication and cookies).
    :type headers: dict, optional
    """

    _MAX_POST_COUNT = 100  # max seems to be 100 posts per page
    _MAX_UPDATE_COUNT = 100  # max seems to be 100
    _MAX_SEARCH_COUNT = 49  # max seems to be 49, and min seems to be 2
    _MAX_REPEATED_REQUESTS = (
        200  # VERY conservative max requests count to avoid rate-limit
    )

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        *,
        authenticate=True,
        refresh_cookies=False,
        debug=False,
        proxy: Optional[str] = None,
        cookies=None,
        cookies_dir: str = "",
        headers: Optional[Dict[str, str]] = None,
    ):
        """Constructor method"""
        proxy_config = {}
        if proxy:
            try:
                # Expected format: host:port:username:password
                parts = proxy.split(':')
                if len(parts) == 4:
                    host, port, username, password = parts
                    proxy_url = f"http://{username}:{password}@{host}:{port}"
                    proxy_config = {'http': proxy_url, 'https': proxy_url}
                else:
                    logger.error("Invalid proxy format. Expected 'host:port:username:password'")
            except Exception as e:
                logger.error(f"Error parsing proxy string: {e}")
        self.client = Client(
            refresh_cookies=refresh_cookies,
            debug=debug,
            proxies=proxy_config,
            cookies_dir=cookies_dir,
        )
        logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
        self.logger = logger

        self.custom_headers = headers  # Store custom headers
        self.proxy = proxy  # Store proxy string
        self.proxy_config = proxy_config  # Store proxy configuration for requests

        if headers:
            # Skip authentication and cookie checks if headers are provided
            self.logger.info("Using custom headers. Skipping authentication.")
        elif authenticate:
            if cookies:
                # If the cookies are expired, the API won't work anymore since
                # `username` and `password` are not used at all in this case.
                self.client._set_session_cookies(cookies)
            elif username and password:
                self.client.authenticate(username, password)
            else:
                raise ValueError("Either headers or username/password must be provided.")

    def _fetch(self, uri: str, evade=default_evade, base_request=False, **kwargs):
        """GET request to Linkedin API"""
        evade()

        url = f"{self.client.API_BASE_URL if not base_request else self.client.LINKEDIN_BASE_URL}{uri}"
        if self.proxy_config:
            kwargs['proxies'] = self.proxy_config  # Apply proxy with authentication
        headers = self.custom_headers or self.client.REQUEST_HEADERS
        return self.client.session.get(url, headers=headers, **kwargs)

    def safe_get(self,data, *keys, default=''):
        """Safely access nested dictionary keys, return default if any key is missing or None."""
        current = data
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key, default)
            if current is None:
                return default
        return current

    def is_linkedin_company_url(self,url: str) -> bool:
        return "linkedin.com/company" in url

    def extract_linkedin_company_id(self,url: str) -> str:
        if "linkedin.com/company" not in url:
            return None
        try:
            parts = url.split("linkedin.com/company/")[1]
            return parts.split('/')[0]  # Extracts slug or ID
        except IndexError:
            return None
    def _cookies(self):
        """Return client cookies"""
        return self.client.cookies

    def flatten_linkedin_profile(self,profile):
        """
        Flattens LinkedIn JSON data into a single-row DataFrame with all data in separate columns

        Parameters:
        json_str (str): JSON string containing LinkedIn profile data

        Returns:
        pd.DataFrame: Single-row DataFrame with all data in separate columns
        """
        data = profile
        flat = {}

        # Flatten profile details
        for k, v in data['profile_details'].items():
            flat[k] = v

        # Flatten experiences
        for idx, exp in enumerate(data['experience']):
            for key, val in exp.items():
                flat[f'exp_{idx}_{key}'] = val

        # Flatten education
        for idx, edu in enumerate(data['education']):
            for key, val in edu.items():
                flat[f'edu_{idx}_{key}'] = val

        # Flatten skills

        flat[f'skill'] = data['skills']

        # Flatten image URL
        flat['image_url'] = data.get('image', '')

        # Add empty sections for completeness
        for section in ['certifications', 'honors', 'test_scores', 'languages',
                        'volunteer_experiences', 'projects', 'publications', 'courses']:
            if data.get(section):
                for idx, item in enumerate(data[section]):
                    if isinstance(item, dict):
                        for k, v in item.items():
                            flat[f'{section}_{idx}_{k}'] = v
                    else:
                        flat[f'{section}_{idx}'] = item

        return pd.DataFrame([flat])
    def _headers(self):
        """Return client cookies"""
        return self.client.REQUEST_HEADERS

    def _post(self, uri: str, evade=default_evade, base_request=False, **kwargs):
        """POST request to Linkedin API"""
        evade()

        url = f"{self.client.API_BASE_URL if not base_request else self.client.LINKEDIN_BASE_URL}{uri}"
        if self.proxy_config:
            kwargs['proxies'] = self.proxy_config  # Apply proxy with authentication
        headers = self.custom_headers or self.client.REQUEST_HEADERS
        return self.client.session.post(url, headers=headers, **kwargs)
    def safe_split_urn(self,urn, delimiter=':', default=''):
        """
        Safely splits a URN string and returns the last segment, handling None or invalid inputs.
        """
        if urn and isinstance(urn, str):
            return urn.split(delimiter)[-1]
        return default
    def get_profile_posts(
        self,
        public_id: Optional[str] = None,
        urn_id: Optional[str] = None,
        post_count=10,
    ) -> List:
        """
        get_profile_posts: Get profile posts

        :param public_id: LinkedIn public ID for a profile
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str, optional
        :param post_count: Number of posts to fetch
        :type post_count: int, optional
        :return: List of posts
        :rtype: list
        """
        url_params = {
            "count": min(post_count, self._MAX_POST_COUNT),
            "start": 0,
            "q": "memberShareFeed",
            "moduleKey": "member-shares:phone",
            "includeLongTermHistory": True,
        }
        if urn_id:
            profile_urn = f"urn:li:fsd_profile:{urn_id}"
        else:
            profile = self.get_profile(public_id=public_id)
            profile_urn = profile["profile_urn"].replace(
                "fs_miniProfile", "fsd_profile"
            )
        url_params["profileUrn"] = profile_urn
        url = f"/identity/profileUpdatesV2"
        res = self._fetch(url, params=url_params)
        data = res.json()
        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data["message"]))
            return [{}]
        while data and data["metadata"]["paginationToken"] != "":
            if len(data["elements"]) >= post_count:
                break
            pagination_token = data["metadata"]["paginationToken"]
            url_params["start"] = url_params["start"] + self._MAX_POST_COUNT
            url_params["paginationToken"] = pagination_token
            res = self._fetch(url, params=url_params)
            data["metadata"] = res.json()["metadata"]
            data["elements"] = data["elements"] + res.json()["elements"]
            data["paging"] = res.json()["paging"]
        return data["elements"]

    def get_post_comments(self, post_urn: str, comment_count=100) -> List:
        """
        get_post_comments: Get post comments

        :param post_urn: Post URN
        :type post_urn: str
        :param comment_count: Number of comments to fetch
        :type comment_count: int, optional
        :return: List of post comments
        :rtype: list
        """
        url_params = {
            "count": min(comment_count, self._MAX_POST_COUNT),
            "start": 0,
            "q": "comments",
            "sortOrder": "RELEVANCE",
        }
        url = f"/feed/comments"
        url_params["updateId"] = "activity:" + post_urn
        res = self._fetch(url, params=url_params)
        data = res.json()
        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data["status"]))
            return [{}]
        while data and data["metadata"]["paginationToken"] != "":
            if len(data["elements"]) >= comment_count:
                break
            pagination_token = data["metadata"]["paginationToken"]
            url_params["start"] = url_params["start"] + self._MAX_POST_COUNT
            url_params["count"] = self._MAX_POST_COUNT
            url_params["paginationToken"] = pagination_token
            res = self._fetch(url, params=url_params)
            if res.json() and "status" in res.json() and res.json()["status"] != 200:
                self.logger.info("request failed: {}".format(data["status"]))
                return [{}]
            data["metadata"] = res.json()["metadata"]
            """ When the number of comments exceed total available 
            comments, the api starts returning an empty list of elements"""
            if res.json()["elements"] and len(res.json()["elements"]) == 0:
                break
            if data["elements"] and len(res.json()["elements"]) == 0:
                break
            data["elements"] = data["elements"] + res.json()["elements"]
            data["paging"] = res.json()["paging"]
        return data["elements"]

    def search(self, params: Dict, limit=-1, offset=0) -> List:
        """Perform a LinkedIn search.

        :param params: Search parameters (see code)
        :type params: dict
        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional
        :param offset: Index to start searching from
        :type offset: int, optional


        :return: List of search results
        :rtype: list
        """
        count = Linkedin._MAX_SEARCH_COUNT
        if limit is None:
            limit = -1

        results = []
        while True:
            # when we're close to the limit, only fetch what we need to
            if limit > -1 and limit - len(results) < count:
                count = limit - len(results)
            default_params = {
                "count": str(count),
                "filters": "List()",
                "origin": "GLOBAL_SEARCH_HEADER",
                "q": "all",
                "start": len(results) + offset,
                "queryContext": "List(spellCorrectionEnabled->true,relatedSearchesEnabled->true,kcardTypes->PROFILE|COMPANY)",
                "includeWebMetadata": "true",
            }
            default_params.update(params)

            keywords = (
                f"keywords:{default_params['keywords']},"
                if "keywords" in default_params
                else ""
            )

            res = self._fetch(
                f"/graphql?variables=(start:{default_params['start']},origin:{default_params['origin']},"
                f"query:("
                f"{keywords}"
                f"flagshipSearchIntent:SEARCH_SRP,"
                f"queryParameters:{default_params['filters']},"
                f"includeFiltersInResponse:false))&queryId=voyagerSearchDashClusters"
                f".b0928897b71bd00a5a7291755dcd64f0"
            )
            data = res.json()

            data_clusters = data.get("data", {}).get("searchDashClustersByAll", [])

            if not data_clusters:
                return []

            if (
                not data_clusters.get("_type", [])
                == "com.linkedin.restli.common.CollectionResponse"
            ):
                return []

            new_elements = []
            for it in data_clusters.get("elements", []):
                if (
                    not it.get("_type", [])
                    == "com.linkedin.voyager.dash.search.SearchClusterViewModel"
                ):
                    continue

                for el in it.get("items", []):
                    if (
                        not el.get("_type", [])
                        == "com.linkedin.voyager.dash.search.SearchItem"
                    ):
                        continue

                    e = el.get("item", {}).get("entityResult", [])
                    if not e:
                        continue
                    if (
                        not e.get("_type", [])
                        == "com.linkedin.voyager.dash.search.EntityResultViewModel"
                    ):
                        continue
                    new_elements.append(e)

            results.extend(new_elements)

            # break the loop if we're done searching
            # NOTE: we could also check for the `total` returned in the response.
            # This is in data["data"]["paging"]["total"]
            if (
                (-1 < limit <= len(results))  # if our results exceed set limit
                or len(results) / count >= Linkedin._MAX_REPEATED_REQUESTS
            ) or len(new_elements) == 0:
                break

            self.logger.debug(f"results grew to {len(results)}")

        return results

    def search_people(
        self,
        keywords: Optional[str] = None,
        connection_of: Optional[str] = None,
        network_depths: Optional[
            List[Union[Literal["F"], Literal["S"], Literal["O"]]]
        ] = None,
        current_company: Optional[List[str]] = None,
        past_companies: Optional[List[str]] = None,
        nonprofit_interests: Optional[List[str]] = None,
        profile_languages: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        industries: Optional[List[str]] = None,
        schools: Optional[List[str]] = None,
        contact_interests: Optional[List[str]] = None,
        service_categories: Optional[List[str]] = None,
        include_private_profiles=False,  # profiles without a public id, "Linkedin Member"
        # Keywords filter
        keyword_first_name: Optional[str] = None,
        keyword_last_name: Optional[str] = None,
        # `keyword_title` and `title` are the same. We kept `title` for backward compatibility. Please only use one of them.
        keyword_title: Optional[str] = None,
        keyword_company: Optional[str] = None,
        keyword_school: Optional[str] = None,
        network_depth: Optional[
            Union[Literal["F"], Literal["S"], Literal["O"]]
        ] = None,  # DEPRECATED - use network_depths
        title: Optional[str] = None,  # DEPRECATED - use keyword_title
        **kwargs,
    ) -> List[Dict]:
        """Perform a LinkedIn search for people.

        :param keywords: Keywords to search on
        :type keywords: str, optional
        :param current_company: A list of company URN IDs (str)
        :type current_company: list, optional
        :param past_companies: A list of company URN IDs (str)
        :type past_companies: list, optional
        :param regions: A list of geo URN IDs (str)
        :type regions: list, optional
        :param industries: A list of industry URN IDs (str)
        :type industries: list, optional
        :param schools: A list of school URN IDs (str)
        :type schools: list, optional
        :param profile_languages: A list of 2-letter language codes (str)
        :type profile_languages: list, optional
        :param contact_interests: A list containing one or both of "proBono" and "boardMember"
        :type contact_interests: list, optional
        :param service_categories: A list of service category URN IDs (str)
        :type service_categories: list, optional
        :param network_depth: Deprecated, use `network_depths`. One of "F", "S" and "O" (first, second and third+ respectively)
        :type network_depth: str, optional
        :param network_depths: A list containing one or many of "F", "S" and "O" (first, second and third+ respectively)
        :type network_depths: list, optional
        :param include_private_profiles: Include private profiles in search results. If False, only public profiles are included. Defaults to False
        :type include_private_profiles: boolean, optional
        :param keyword_first_name: First name
        :type keyword_first_name: str, optional
        :param keyword_last_name: Last name
        :type keyword_last_name: str, optional
        :param keyword_title: Job title
        :type keyword_title: str, optional
        :param keyword_company: Company name
        :type keyword_company: str, optional
        :param keyword_school: School name
        :type keyword_school: str, optional
        :param connection_of: Connection of LinkedIn user, given by profile URN ID
        :type connection_of: str, optional
        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional

        :return: List of profiles (minimal data only)
        :rtype: list
        """
        filters = ["(key:resultType,value:List(PEOPLE))"]
        if connection_of:
            filters.append(f"(key:connectionOf,value:List({connection_of}))")
        if network_depths:
            stringify = " | ".join(network_depths)
            filters.append(f"(key:network,value:List({stringify}))")
        elif network_depth:
            filters.append(f"(key:network,value:List({network_depth}))")
        if regions:
            stringify = " | ".join(regions)
            filters.append(f"(key:geoUrn,value:List({stringify}))")
        if industries:
            stringify = " | ".join(industries)
            filters.append(f"(key:industry,value:List({stringify}))")
        if current_company:
            stringify = " | ".join(current_company)
            filters.append(f"(key:currentCompany,value:List({stringify}))")
        if past_companies:
            stringify = " | ".join(past_companies)
            filters.append(f"(key:pastCompany,value:List({stringify}))")
        if profile_languages:
            stringify = " | ".join(profile_languages)
            filters.append(f"(key:profileLanguage,value:List({stringify}))")
        if nonprofit_interests:
            stringify = " | ".join(nonprofit_interests)
            filters.append(f"(key:nonprofitInterest,value:List({stringify}))")
        if schools:
            stringify = " | ".join(schools)
            filters.append(f"(key:schools,value:List({stringify}))")
        if service_categories:
            stringify = " | ".join(service_categories)
            filters.append(f"(key:serviceCategory,value:List({stringify}))")
        # `Keywords` filter
        keyword_title = keyword_title if keyword_title else title
        if keyword_first_name:
            filters.append(f"(key:firstName,value:List({keyword_first_name}))")
        if keyword_last_name:
            filters.append(f"(key:lastName,value:List({keyword_last_name}))")
        if keyword_title:
            filters.append(f"(key:title,value:List({keyword_title}))")
        if keyword_company:
            filters.append(f"(key:company,value:List({keyword_company}))")
        if keyword_school:
            filters.append(f"(key:school,value:List({keyword_school}))")

        params = {"filters": "List({})".format(",".join(filters))}

        if keywords:
            params["keywords"] = keywords

        data = self.search(params, **kwargs)

        results = []
        for item in data:
            if (
                not include_private_profiles
                and (item.get("entityCustomTrackingInfo") or {}).get(
                    "memberDistance", None
                )
                == "OUT_OF_NETWORK"
            ):
                continue
            results.append(
                {
                    "urn_id": get_id_from_urn(
                        get_urn_from_raw_update(item.get("entityUrn", None))
                    ),
                    "distance": (item.get("entityCustomTrackingInfo") or {}).get(
                        "memberDistance", None
                    ),
                    "jobtitle": (item.get("primarySubtitle") or {}).get("text", None),
                    "location": (item.get("secondarySubtitle") or {}).get("text", None),
                    "name": (item.get("title") or {}).get("text", None),
                }
            )

        return results

    def search_companies(self, keywords: Optional[List[str]] = None, **kwargs) -> List:
        """Perform a LinkedIn search for companies.

        :param keywords: A list of search keywords (str)
        :type keywords: list, optional

        :return: List of companies
        :rtype: list
        """
        filters = ["(key:resultType,value:List(COMPANIES))"]

        params: Dict[str, Union[str, List[str]]] = {
            "filters": "List({})".format(",".join(filters)),
            "queryContext": "List(spellCorrectionEnabled->true)",
        }

        if keywords:
            params["keywords"] = keywords

        data = self.search(params, **kwargs)

        results = []
        for item in data:
            if "company" not in item.get("trackingUrn"):
                continue
            results.append(
                {
                    "urn_id": get_id_from_urn(item.get("trackingUrn", None)),
                    "name": (item.get("title") or {}).get("text", None),
                    "headline": (item.get("primarySubtitle") or {}).get("text", None),
                    "subline": (item.get("secondarySubtitle") or {}).get("text", None),
                }
            )

        return results

    def search_jobs(
        self,
        keywords: Optional[str] = None,
        companies: Optional[List[str]] = None,
        experience: Optional[
            List[
                Union[
                    Literal["1"],
                    Literal["2"],
                    Literal["3"],
                    Literal["4"],
                    Literal["5"],
                    Literal["6"],
                ]
            ]
        ] = None,
        job_type: Optional[
            List[
                Union[
                    Literal["F"],
                    Literal["C"],
                    Literal["P"],
                    Literal["T"],
                    Literal["I"],
                    Literal["V"],
                    Literal["O"],
                ]
            ]
        ] = None,
        job_title: Optional[List[str]] = None,
        industries: Optional[List[str]] = None,
        location_name: Optional[str] = None,
        remote: Optional[List[Union[Literal["1"], Literal["2"], Literal["3"]]]] = None,
        listed_at=24 * 60 * 60,
        distance: Optional[int] = None,
        limit=-1,
        offset=0,
        **kwargs,
    ) -> List[Dict]:
        """Perform a LinkedIn search for jobs.

        :param keywords: Search keywords (str)
        :type keywords: str, optional
        :param companies: A list of company URN IDs (str)
        :type companies: list, optional
        :param experience: A list of experience levels, one or many of "1", "2", "3", "4", "5" and "6" (internship, entry level, associate, mid-senior level, director and executive, respectively)
        :type experience: list, optional
        :param job_type:  A list of job types , one or many of "F", "C", "P", "T", "I", "V", "O" (full-time, contract, part-time, temporary, internship, volunteer and "other", respectively)
        :type job_type: list, optional
        :param job_title: A list of title URN IDs (str)
        :type job_title: list, optional
        :param industries: A list of industry URN IDs (str)
        :type industries: list, optional
        :param location_name: Name of the location to search within. Example: "Kyiv City, Ukraine"
        :type location_name: str, optional
        :param remote: Filter for remote jobs, onsite or hybrid. onsite:"1", remote:"2", hybrid:"3"
        :type remote: list, optional
        :param listed_at: maximum number of seconds passed since job posting. 86400 will filter job postings posted in last 24 hours.
        :type listed_at: int/str, optional. Default value is equal to 24 hours.
        :param distance: maximum distance from location in miles
        :type distance: int/str, optional. If not specified, None or 0, the default value of 25 miles applied.
        :param limit: maximum number of results obtained from API queries. -1 means maximum which is defined by constants and is equal to 1000 now.
        :type limit: int, optional, default -1
        :param offset: indicates how many search results shall be skipped
        :type offset: int, optional
        :return: List of jobs
        :rtype: list
        """
        count = Linkedin._MAX_SEARCH_COUNT
        if limit is None:
            limit = -1

        query: Dict[str, Union[str, Dict[str, str]]] = {
            "origin": "JOB_SEARCH_PAGE_QUERY_EXPANSION"
        }
        if keywords:
            query["keywords"] = "KEYWORD_PLACEHOLDER"
        if location_name:
            query["locationFallback"] = "LOCATION_PLACEHOLDER"

        # In selectedFilters()
        query["selectedFilters"] = {}
        if companies:
            query["selectedFilters"]["company"] = f"List({','.join(companies)})"
        if experience:
            query["selectedFilters"]["experience"] = f"List({','.join(experience)})"
        if job_type:
            query["selectedFilters"]["jobType"] = f"List({','.join(job_type)})"
        if job_title:
            query["selectedFilters"]["title"] = f"List({','.join(job_title)})"
        if industries:
            query["selectedFilters"]["industry"] = f"List({','.join(industries)})"
        if distance:
            query["selectedFilters"]["distance"] = f"List({distance})"
        if remote:
            query["selectedFilters"]["workplaceType"] = f"List({','.join(remote)})"

        query["selectedFilters"]["timePostedRange"] = f"List(r{listed_at})"
        query["spellCorrectionEnabled"] = "true"

        # Query structure:
        # "(
        #    origin:JOB_SEARCH_PAGE_QUERY_EXPANSION,
        #    keywords:marketing%20manager,
        #    locationFallback:germany,
        #    selectedFilters:(
        #        distance:List(25),
        #        company:List(163253),
        #        salaryBucketV2:List(5),
        #        timePostedRange:List(r2592000),
        #        workplaceType:List(1)
        #    ),
        #    spellCorrectionEnabled:true
        #  )"

        query_string = (
            str(query)
            .replace(" ", "")
            .replace("'", "")
            .replace("KEYWORD_PLACEHOLDER", keywords or "")
            .replace("LOCATION_PLACEHOLDER", location_name or "")
            .replace("{", "(")
            .replace("}", ")")
        )
        results = []
        while True:
            # when we're close to the limit, only fetch what we need to
            if limit > -1 and limit - len(results) < count:
                count = limit - len(results)
            default_params = {
                "decorationId": "com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-174",
                "count": count,
                "q": "jobSearch",
                "query": query_string,
                "start": len(results) + offset,
            }

            res = self._fetch(
                f"/voyagerJobsDashJobCards?{urlencode(default_params, safe='(),:')}",
                headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
            )
            data = res.json()

            elements = data.get("included", [])
            new_data = [
                i
                for i in elements
                if i["$type"] == "com.linkedin.voyager.dash.jobs.JobPosting"
            ]
            # break the loop if we're done searching or no results returned
            if not new_data:
                break
            # NOTE: we could also check for the `total` returned in the response.
            # This is in data["data"]["paging"]["total"]
            results.extend(new_data)
            if (
                (-1 < limit <= len(results))  # if our results exceed set limit
                or len(results) / count >= Linkedin._MAX_REPEATED_REQUESTS
            ) or len(elements) == 0:
                break

            self.logger.debug(f"results grew to {len(results)}")

        return results

    def get_profile_contact_info(
        self, public_id: Optional[str] = None, urn_id: Optional[str] = None
    ) -> Dict:
        """Fetch contact information for a given LinkedIn profile. Pass a [public_id] or a [urn_id].

        :param public_id: LinkedIn public ID for a profile
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str, optional

        :return: Contact data
        :rtype: dict
        """
        res = self._fetch(
            f"/identity/profiles/{public_id or urn_id}/profileContactInfo"
        )
        data = res.json()
        data=data['data']
        contact_info = {
            "email_address": data.get("emailAddress"),
            "websites": [],
            "twitter": data.get("twitterHandles"),
            "birthdate": data.get("birthDateOn"),
            "ims": data.get("ims"),
            "phone_numbers": data.get("phoneNumbers", []),
        }

        websites = data.get("websites", [])
        if websites:
            for item in websites:
                if "com.linkedin.voyager.identity.profile.StandardWebsite" in item["type"]:
                    item["label"] = item["type"][
                        "com.linkedin.voyager.identity.profile.StandardWebsite"
                    ]["category"]
                elif "" in item["type"]:
                    item["label"] = item["type"][
                        "com.linkedin.voyager.identity.profile.CustomWebsite"
                    ]["label"]

                del item["type"]

        contact_info["websites"] = websites

        return contact_info

    def get_profile_skills(
        self, public_id: Optional[str] = None, urn_id: Optional[str] = None
    ) -> List:
        """Fetch the skills listed on a given LinkedIn profile.

        :param public_id: LinkedIn public ID for a profile
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str, optional


        :return: List of skill objects
        :rtype: list
        """
        params = {"count": 100, "start": 0}
        res = self._fetch(
            f"/identity/profiles/{public_id or urn_id}/skills", params=params
        )
        data = res.json()

        skills = data.get("included", [])
        for item in skills:
            if "entityUrn" in item:
                del item["entityUrn"]
            if "$type" in item:
                del item["$type"]
            if "standardizedSkill" in item:
                del item["standardizedSkill"]
            if "standardizedSkillUrn" in item:
                del item["standardizedSkillUrn"]
        skills = [s["name"] for s in skills]
        return skills

    def extract_linkedin_profile(self, json_data):
        """
        Extracts detailed LinkedIn profile information from JSON data, including IDs.
        Returns a dictionary with profile details, experience, education, certifications, skills, image, and more.
        """
        result = {
            'profile_details': {},
            'experience': [],
            'education': [],
            'certifications': [],
            'skills': [],
            'image': '',
            'honors': [],
            'test_scores': [],
            'languages': [],
            'volunteer_experiences': [],
            'projects': [],
            'publications': [],
            'courses': []
        }

        included = json_data.get('included', [])
        # Extract paging for experiences and skills
        position_view = [item for item in included if
                         item.get('$type') == 'com.linkedin.voyager.identity.profile.PositionView']
        skill_view = [item for item in included if
                      item.get('$type') == 'com.linkedin.voyager.identity.profile.SkillView']

        fetch_more_experiences = False
        fetch_more_skills = False
        profile_id_for_fetch = None

        for item in position_view:
            count = self.safe_get(item, 'paging', 'count', default=0)
            total = self.safe_get(item, 'paging', 'total', default=0)
            profile_id = self.safe_get(item, 'profileId', default='')
            if count < total and profile_id:
                fetch_more_experiences = True
                profile_id_for_fetch = profile_id

        for item in skill_view:
            count = self.safe_get(item, 'paging', 'count', default=0)
            total = self.safe_get(item, 'paging', 'total', default=0)
            profile_id = self.safe_get(item, 'profileId', default='')
            if count < total and profile_id:
                fetch_more_skills = True
                profile_id_for_fetch = profile_id

        for item in included:
            item_type = self.safe_get(item, '$type', default='')

            if item_type == 'com.linkedin.voyager.identity.profile.Profile':
                result['profile_details'] = {
                    'linkedInIdentifier': self.safe_split_urn(self.safe_get(item, 'entityUrn')),
                    'first_name': self.safe_get(item, 'firstName'),
                    'last_name': self.safe_get(item, 'lastName'),
                    'headline': self.safe_get(item, 'headline'),
                    'summary': self.safe_get(item, 'summary'),
                    'location': self.safe_get(item, 'locationName'),
                    'industry': self.safe_get(item, 'industryName'),
                    'address': self.safe_get(item, 'address'),
                    'geo_location': self.safe_get(item, 'geoLocationName'),
                    'geo_country': self.safe_get(item, 'geoCountryName'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                    'openToWork': self.safe_get(item, 'elt'),
                }

            elif item_type == 'com.linkedin.voyager.identity.profile.Position':
                if fetch_more_experiences:
                    pass
                else:
                    start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                    end_date = self.safe_get(item, 'timePeriod', 'endDate', default={})
                    company = self.safe_get(item, 'company', default={})
                    experience = {
                        'title': self.safe_get(item, 'title'),
                        'company': self.safe_get(item, 'companyName'),
                        'company_id': self.safe_split_urn(self.safe_get(item, 'companyUrn')),
                        'company_linked': f"https://linkedin.com/company/{self.safe_split_urn(self.safe_get(item, 'companyUrn'))}",
                        'location': self.safe_get(item, 'locationName'),
                        'geo_location': self.safe_get(item, 'geoLocationName'),
                        'start_date': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                        'end_date': f"{self.safe_get(end_date, 'month')}/{self.safe_get(end_date, 'year')}" if end_date else 'Present',
                        'description': self.safe_get(item, 'description'),
                        'industries': self.safe_get(company, 'industries', default=[]),
                        'entity_urn': self.safe_get(item, 'entityUrn'),
                        'employee_count_range': self.safe_get(company, 'employeeCountRange', 'start', default=None),
                    }
                    result['experience'].append(experience)

            elif item_type == 'com.linkedin.voyager.identity.profile.Education':
                time_period = self.safe_get(item, 'timePeriod', default={})
                education = {
                    'school': self.safe_get(item, 'schoolName'),
                    'school_id': self.safe_split_urn(self.safe_get(item, 'schoolUrn')),
                    'degree': self.safe_get(item, 'degreeName'),
                    'field_of_study': self.safe_get(item, 'fieldOfStudy'),
                    'field_of_study_urn': self.safe_get(item, 'fieldOfStudyUrn'),
                    'degree_urn': self.safe_get(item, 'degreeUrn'),
                    'start_year': self.safe_get(time_period, 'startDate', 'year'),
                    'end_year': self.safe_get(time_period, 'endDate', 'year'),
                    'description': self.safe_get(item, 'description'),
                    'activities': self.safe_get(item, 'activities'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['education'].append(education)

            elif item_type == 'com.linkedin.voyager.identity.profile.Certification':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                certification = {
                    'name': self.safe_get(item, 'name'),
                    'authority': self.safe_get(item, 'authority'),
                    'issued': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'url': self.safe_get(item, 'url'),
                    'license_number': self.safe_get(item, 'licenseNumber'),
                    'display_source': self.safe_get(item, 'displaySource'),
                    'company_id': self.safe_split_urn(self.safe_get(item, 'companyUrn')),
                    'company_linked': f"https://linkedin.com/company/{self.safe_split_urn(self.safe_get(item, 'companyUrn'))}",
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['certifications'].append(certification)

            elif item_type == 'com.linkedin.voyager.identity.profile.Skill':
                if fetch_more_skills:
                    pass
                else:
                    skill = {
                        'name': self.safe_get(item, 'name'),
                        'standardized_skill_urn': self.safe_get(item, 'standardizedSkillUrn'),
                    }
                    result['skills'].append(skill)

            elif item_type == 'com.linkedin.voyager.identity.shared.MiniProfile':
                picture = self.safe_get(item, 'picture', default={})
                artifacts = self.safe_get(picture, 'artifacts', default=[])
                if artifacts:
                    largest = max(artifacts, key=lambda x: self.safe_get(x, 'width', default=0))
                    result['image'] = self.safe_get(picture, 'rootUrl', default='') + self.safe_get(largest,
                                                                                          'fileIdentifyingUrlPathSegment',
                                                                                          default='')
                result['profile_details'].update({
                    'publicIdentifier': self.safe_get(item, 'publicIdentifier'),
                    'tracking_id': self.safe_get(item, 'trackingId'),
                    'memberIdentifier': self.safe_split_urn(self.safe_get(item, 'objectUrn')),
                })

            elif item_type == 'com.linkedin.voyager.identity.profile.Honor':
                honor = {
                    'title': self.safe_get(item, 'title'),
                    'issuer': self.safe_get(item, 'issuer'),
                    'date': self.safe_get(item, 'issueDate'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['honors'].append(honor)

            elif item_type == 'com.linkedin.voyager.identity.profile.TestScore':
                test_score = {
                    'name': self.safe_get(item, 'name'),
                    'score': self.safe_get(item, 'score'),
                    'description': self.safe_get(item, 'description'),
                    'date': f"{self.safe_get(item, 'date', 'month')}/{self.safe_get(item, 'date', 'year')}" if self.safe_get(item,
                                                                                                              'date') else '',
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['test_scores'].append(test_score)

            elif item_type == 'com.linkedin.voyager.identity.profile.Language':
                language = {
                    'name': self.safe_get(item, 'name'),
                    'proficiency': self.safe_get(item, 'proficiency'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['languages'].append(language)

            elif item_type == 'com.linkedin.voyager.identity.profile.VolunteerExperience':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                end_date = self.safe_get(item, 'timePeriod', 'endDate', default={})
                volunteer = {
                    'role': self.safe_get(item, 'role'),
                    'organization': self.safe_get(item, 'companyName'),
                    'cause': self.safe_get(item, 'cause'),
                    'start_date': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'end_date': f"{self.safe_get(end_date, 'month')}/{self.safe_get(end_date, 'year')}" if end_date else 'Present',
                    'description': self.safe_get(item, 'description'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['volunteer_experiences'].append(volunteer)

            elif item_type == 'com.linkedin.voyager.identity.profile.Project':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                end_date = self.safe_get(item, 'timePeriod', 'endDate', default={})
                project = {
                    'title': self.safe_get(item, 'title'),
                    'description': self.safe_get(item, 'description'),
                    'start_date': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'end_date': f"{self.safe_get(end_date, 'month')}/{self.safe_get(end_date, 'year')}" if end_date else 'Present',
                    'url': self.safe_get(item, 'url'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['projects'].append(project)

            elif item_type == 'com.linkedin.voyager.identity.profile.Publication':
                publication = {
                    'title': self.safe_get(item, 'name'),
                    'publisher': self.safe_get(item, 'publisher'),
                    'description': self.safe_get(item, 'description'),
                    'date': f"{self.safe_get(item, 'date', 'month')}/{self.safe_get(item, 'date', 'year')}" if self.safe_get(item,
                                                                                                              'date') else '',
                    'url': self.safe_get(item, 'url'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['publications'].append(publication)

            elif item_type == 'com.linkedin.voyager.identity.profile.Course':
                course = {
                    'name': self.safe_get(item, 'name'),
                    'number': self.safe_get(item, 'number'),
                    'description': self.safe_get(item, 'description'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['courses'].append(course)

        # Fetch additional data if needed
        if fetch_more_experiences and profile_id_for_fetch:
            time.sleep(3)
            experience = self.get_profile_experiences(profile_id_for_fetch)
            if isinstance(experience, list):
                result['experience'].extend(experience)
            elif isinstance(experience, dict):
                result['experience'].append(experience)

        if fetch_more_skills and profile_id_for_fetch:
            time.sleep(3)
            skills = self.get_profile_skills(profile_id_for_fetch)
            if isinstance(skills, list):
                result['skills'].extend(skills)
            elif isinstance(skills, dict):
                result['skills'].append(skills)

        return result
    def get_profile(
        self, public_id: Optional[str] = None, urn_id: Optional[str] = None
    ) -> Dict:
        """Fetch data for a given LinkedIn profile.

        :param public_id: LinkedIn public ID for a profile
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str, optional

        :return: Profile data
        :rtype: dict
        """
        # NOTE this still works for now, but will probably eventually have to be converted to
        # https://www.linkedin.com/voyager/api/identity/profiles/ACoAAAKT9JQBsH7LwKaE9Myay9WcX8OVGuDq9Uw
        res = self._fetch(f"/identity/profiles/{public_id or urn_id}/profileView")


        if res.status_code != 200:
            self.logger.info("request failed: {}".format(res.content))
            return {}
        data = res.json()
        profile=self.extract_linkedin_profile(data)

        return profile
    def extract_linkedin_profile_without(self, json_data):
        """
        Extracts detailed LinkedIn profile information from JSON data, including IDs.
        Returns a dictionary with profile details, experience, education, certifications, skills, image, and more.
        """
        result = {
            'profile_details': {},
            'experience': [],
            'education': [],
            'certifications': [],
            'skills': [],
            'image': '',
            'honors': [],
            'test_scores': [],
            'languages': [],
            'volunteer_experiences': [],
            'projects': [],
            'publications': [],
            'courses': []
        }

        included = json_data.get('included', [])
        # # Extract paging for experiences and skills
        # position_view = [item for item in included if
        #                  item.get('$type') == 'com.linkedin.voyager.identity.profile.PositionView']
        # skill_view = [item for item in included if
        #               item.get('$type') == 'com.linkedin.voyager.identity.profile.SkillView']
        #
        # fetch_more_experiences = False
        # fetch_more_skills = False
        # profile_id_for_fetch = None
        #
        # # for item in position_view:
        # #     count = self.safe_get(item, 'paging', 'count', default=0)
        # #     total = self.safe_get(item, 'paging', 'total', default=0)
        # #     profile_id = self.safe_get(item, 'profileId', default='')
        # #     if count < total and profile_id:
        # #         fetch_more_experiences = True
        # #         profile_id_for_fetch = profile_id
        # #
        # # for item in skill_view:
        # #     count = self.safe_get(item, 'paging', 'count', default=0)
        # #     total = self.safe_get(item, 'paging', 'total', default=0)
        # #     profile_id = self.safe_get(item, 'profileId', default='')
        # #     if count < total and profile_id:
        # #         fetch_more_skills = True
        # #         profile_id_for_fetch = profile_id

        for item in included:
            item_type = self.safe_get(item, '$type', default='')

            if item_type == 'com.linkedin.voyager.identity.profile.Profile':
                result['profile_details'] = {
                    'linkedInIdentifier': self.safe_split_urn(self.safe_get(item, 'entityUrn')),
                    'first_name': self.safe_get(item, 'firstName'),
                    'last_name': self.safe_get(item, 'lastName'),
                    'headline': self.safe_get(item, 'headline'),
                    'summary': self.safe_get(item, 'summary'),
                    'location': self.safe_get(item, 'locationName'),
                    'industry': self.safe_get(item, 'industryName'),
                    'address': self.safe_get(item, 'address'),
                    'geo_location': self.safe_get(item, 'geoLocationName'),
                    'geo_country': self.safe_get(item, 'geoCountryName'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                    'openToWork': self.safe_get(item, 'elt'),
                }

            elif item_type == 'com.linkedin.voyager.identity.profile.Position':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                end_date = self.safe_get(item, 'timePeriod', 'endDate', default={})
                company = self.safe_get(item, 'company', default={})
                experience = {
                    'title': self.safe_get(item, 'title'),
                    'company': self.safe_get(item, 'companyName'),
                    'company_id': self.safe_split_urn(self.safe_get(item, 'companyUrn')),
                    'company_linked': f"https://linkedin.com/company/{self.safe_split_urn(self.safe_get(item, 'companyUrn'))}",
                    'location': self.safe_get(item, 'locationName'),
                    'geo_location': self.safe_get(item, 'geoLocationName'),
                    'start_date': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'end_date': f"{self.safe_get(end_date, 'month')}/{self.safe_get(end_date, 'year')}" if end_date else 'Present',
                    'description': self.safe_get(item, 'description'),
                    'industries': self.safe_get(company, 'industries', default=[]),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                    'employee_count_range': self.safe_get(company, 'employeeCountRange', 'start', default=None),
                }
                result['experience'].append(experience)

            elif item_type == 'com.linkedin.voyager.identity.profile.Education':
                time_period = self.safe_get(item, 'timePeriod', default={})
                education = {
                    'school': self.safe_get(item, 'schoolName'),
                    'school_id': self.safe_split_urn(self.safe_get(item, 'schoolUrn')),
                    'degree': self.safe_get(item, 'degreeName'),
                    'field_of_study': self.safe_get(item, 'fieldOfStudy'),
                    'field_of_study_urn': self.safe_get(item, 'fieldOfStudyUrn'),
                    'degree_urn': self.safe_get(item, 'degreeUrn'),
                    'start_year': self.safe_get(time_period, 'startDate', 'year'),
                    'end_year': self.safe_get(time_period, 'endDate', 'year'),
                    'description': self.safe_get(item, 'description'),
                    'activities': self.safe_get(item, 'activities'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['education'].append(education)

            elif item_type == 'com.linkedin.voyager.identity.profile.Certification':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                certification = {
                    'name': self.safe_get(item, 'name'),
                    'authority': self.safe_get(item, 'authority'),
                    'issued': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'url': self.safe_get(item, 'url'),
                    'license_number': self.safe_get(item, 'licenseNumber'),
                    'display_source': self.safe_get(item, 'displaySource'),
                    'company_id': self.safe_split_urn(self.safe_get(item, 'companyUrn')),
                    'company_linked': f"https://linkedin.com/company/{self.safe_split_urn(self.safe_get(item, 'companyUrn'))}",
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['certifications'].append(certification)

            elif item_type == 'com.linkedin.voyager.identity.profile.Skill':
                skill = {
                    'name': self.safe_get(item, 'name'),
                    'standardized_skill_urn': self.safe_get(item, 'standardizedSkillUrn'),
                }
                result['skills'].append(skill)

            elif item_type == 'com.linkedin.voyager.identity.shared.MiniProfile':
                picture = self.safe_get(item, 'picture', default={})
                artifacts = self.safe_get(picture, 'artifacts', default=[])
                if artifacts:
                    largest = max(artifacts, key=lambda x: self.safe_get(x, 'width', default=0))
                    result['image'] = self.safe_get(picture, 'rootUrl', default='') + self.safe_get(largest,
                                                                                          'fileIdentifyingUrlPathSegment',
                                                                                          default='')
                result['profile_details'].update({
                    'publicIdentifier': self.safe_get(item, 'publicIdentifier'),
                    'tracking_id': self.safe_get(item, 'trackingId'),
                    'memberIdentifier': self.safe_split_urn(self.safe_get(item, 'objectUrn')),
                })

            elif item_type == 'com.linkedin.voyager.identity.profile.Honor':
                honor = {
                    'title': self.safe_get(item, 'title'),
                    'issuer': self.safe_get(item, 'issuer'),
                    'date': self.safe_get(item, 'issueDate'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['honors'].append(honor)

            elif item_type == 'com.linkedin.voyager.identity.profile.TestScore':
                test_score = {
                    'name': self.safe_get(item, 'name'),
                    'score': self.safe_get(item, 'score'),
                    'description': self.safe_get(item, 'description'),
                    'date': f"{self.safe_get(item, 'date', 'month')}/{self.safe_get(item, 'date', 'year')}" if self.safe_get(item,
                                                                                                              'date') else '',
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['test_scores'].append(test_score)

            elif item_type == 'com.linkedin.voyager.identity.profile.Language':
                language = {
                    'name': self.safe_get(item, 'name'),
                    'proficiency': self.safe_get(item, 'proficiency'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['languages'].append(language)

            elif item_type == 'com.linkedin.voyager.identity.profile.VolunteerExperience':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                end_date = self.safe_get(item, 'timePeriod', 'endDate', default={})
                volunteer = {
                    'role': self.safe_get(item, 'role'),
                    'organization': self.safe_get(item, 'companyName'),
                    'cause': self.safe_get(item, 'cause'),
                    'start_date': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'end_date': f"{self.safe_get(end_date, 'month')}/{self.safe_get(end_date, 'year')}" if end_date else 'Present',
                    'description': self.safe_get(item, 'description'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['volunteer_experiences'].append(volunteer)

            elif item_type == 'com.linkedin.voyager.identity.profile.Project':
                start_date = self.safe_get(item, 'timePeriod', 'startDate', default={})
                end_date = self.safe_get(item, 'timePeriod', 'endDate', default={})
                project = {
                    'title': self.safe_get(item, 'title'),
                    'description': self.safe_get(item, 'description'),
                    'start_date': f"{self.safe_get(start_date, 'month')}/{self.safe_get(start_date, 'year')}" if start_date else '',
                    'end_date': f"{self.safe_get(end_date, 'month')}/{self.safe_get(end_date, 'year')}" if end_date else 'Present',
                    'url': self.safe_get(item, 'url'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['projects'].append(project)

            elif item_type == 'com.linkedin.voyager.identity.profile.Publication':
                publication = {
                    'title': self.safe_get(item, 'name'),
                    'publisher': self.safe_get(item, 'publisher'),
                    'description': self.safe_get(item, 'description'),
                    'date': f"{self.safe_get(item, 'date', 'month')}/{self.safe_get(item, 'date', 'year')}" if self.safe_get(item,
                                                                                                              'date') else '',
                    'url': self.safe_get(item, 'url'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['publications'].append(publication)

            elif item_type == 'com.linkedin.voyager.identity.profile.Course':
                course = {
                    'name': self.safe_get(item, 'name'),
                    'number': self.safe_get(item, 'number'),
                    'description': self.safe_get(item, 'description'),
                    'entity_urn': self.safe_get(item, 'entityUrn'),
                }
                result['courses'].append(course)

        # Fetch additional data if needed
        return result
    def get_profile_without(
        self, public_id: Optional[str] = None, urn_id: Optional[str] = None
    ) -> Dict:
        """Fetch data for a given LinkedIn profile.

        :param public_id: LinkedIn public ID for a profile
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str, optional

        :return: Profile data
        :rtype: dict
        """
        # NOTE this still works for now, but will probably eventually have to be converted to
        # https://www.linkedin.com/voyager/api/identity/profiles/ACoAAAKT9JQBsH7LwKaE9Myay9WcX8OVGuDq9Uw
        res = self._fetch(f"/identity/profiles/{public_id or urn_id}/profileView")


        if res.status_code != 200:
            self.logger.info("request failed: {}".format(res.content))
            return {}
        data = res.json()
        profile=self.extract_linkedin_profile(data)

        return profile

    def get_profile_connections(self, urn_id: str, **kwargs) -> List:
        """Fetch connections for a given LinkedIn profile.

        See Linkedin.search_people() for additional searching parameters.

        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str

        :return: List of search results
        :rtype: list
        """
        return self.search_people(connection_of=urn_id, **kwargs)

    def get_profile_experiences(self, urn_id: str) -> List:
        """Fetch experiences for a given LinkedIn profile.

        NOTE: data structure differs slightly from  Linkedin.get_profile() experiences.

        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str

        :return: List of experiences
        :rtype: list
        """
        profile_urn = f"urn:li:fsd_profile:{urn_id}"
        variables = ",".join(
            [f"profileUrn:{quote(profile_urn)}", "sectionType:experience,locale:en_US"]
        )
        query_id = (
            "voyagerIdentityDashProfileComponents.d585e8a2df1f73bcb368cf071109c096"
        )

        res = self._fetch(
            f"/graphql?variables=({variables})&queryId={query_id}&includeWebMetadata=true",

        )

        def parse_item(item, is_group_item=False):
            """
            Parse a single experience item.

            Items as part of an 'experience group' (e.g. a company with multiple positions) have different data structures.
            Therefore, some exceptions need to be made when parsing these items.
            """
            component = item["components"]["entityComponent"]
            title = component["titleV2"]["text"]["text"]
            subtitle = component["subtitle"]
            linkedInUrl=self.is_linkedin_company_url(component['textActionTarget'])
            if linkedInUrl:
                linkedInUrl=component['textActionTarget']
            linkedInId=self.extract_linkedin_company_id(component['textActionTarget'])
            company = subtitle["text"].split(" · ")[0] if subtitle else None
            employment_type_parts = subtitle["text"].split(" · ") if subtitle else None
            employment_type = (
                employment_type_parts[1]
                if employment_type_parts and len(employment_type_parts) > 1
                else None
            )
            metadata = component.get("metadata", {}) or {}
            location = metadata.get("text")

            duration_text = component["caption"]["text"]
            duration_parts = duration_text.split(" · ")
            date_parts = duration_parts[0].split(" - ")

            duration = (
                duration_parts[1]
                if duration_parts and len(duration_parts) > 1
                else None
            )
            start_date = date_parts[0] if date_parts else None
            end_date = date_parts[1] if date_parts and len(date_parts) > 1 else None

            sub_components = component["subComponents"]
            fixed_list_component = (
                sub_components["components"][0]["components"]["fixedListComponent"]
                if sub_components
                else None
            )

            fixed_list_text_component = (
                fixed_list_component["components"][0]["components"]["textComponent"]
                if fixed_list_component
                else None
            )

            # Extract additional description
            description = (
                fixed_list_text_component["text"]["text"]
                if fixed_list_text_component
                else None
            )

            # Create a dictionary with the extracted information
            parsed_data = {
                "title": title,
                "companyName": company if not is_group_item else None,
                "contractType": company if is_group_item else employment_type,
                "companyLocation": location,
                "duration": duration,
                "startDate": start_date,
                "endDate": end_date,
                "description": description,
                'linkedInUrl':linkedInUrl,
                'linkedInId':linkedInId,
                'companyLogo':''
            }

            return parsed_data

        def get_grouped_item_id(item):
            sub_components = item["components"]["entityComponent"]["subComponents"]
            sub_components_components = (
                sub_components["components"][0]["components"]
                if sub_components
                else None
            )
            paged_list_component_id = (
                sub_components_components.get("*pagedListComponent", "")
                if sub_components_components
                else None
            )
            if (
                paged_list_component_id
                and "fsd_profilePositionGroup" in paged_list_component_id
            ):
                pattern = r"urn:li:fsd_profilePositionGroup:\([A-z0-9]+,[A-z0-9]+\)"
                match = re.search(pattern, paged_list_component_id)
                return match.group(0) if match else None

        data = res.json()

        items = []
        for item in data["included"][0]["components"]["elements"]:
            grouped_item_id = get_grouped_item_id(item)
            # if the item is part of a group (e.g. a company with multiple positions),
            # find the group items and parse them.
            if grouped_item_id:
                component = item["components"]["entityComponent"]
                # use the company and location from the main item
                company = component["titleV2"]["text"]["text"]

                location = (
                    component["caption"]["text"] if component["caption"] else None
                )

                # find the group
                group = [
                    i
                    for i in data["included"]
                    if grouped_item_id in i.get("entityUrn", "")
                ]
                if not group:
                    continue
                for group_item in group[0]["components"]["elements"]:
                    parsed_data = parse_item(group_item, is_group_item=True)
                    parsed_data["companyName"] = company
                    parsed_data["locationName"] = location
                    items.append(parsed_data)
                continue

            # else, parse the regular item
            parsed_data = parse_item(item)
            items.append(parsed_data)

        return items

    def get_company_updates(
        self,
        public_id: Optional[str] = None,
        urn_id: Optional[str] = None,
        max_results: Optional[int] = None,
        results: Optional[List] = None,
    ) -> List:
        """Fetch company updates (news activity) for a given LinkedIn company.

        :param public_id: LinkedIn public ID for a company
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a company
        :type urn_id: str, optional

        :return: List of company update objects
        :rtype: list
        """

        if results is None:
            results = []

        params = {
            "companyUniversalName": {public_id or urn_id},
            "q": "companyFeedByUniversalName",
            "moduleKey": "member-share",
            "count": Linkedin._MAX_UPDATE_COUNT,
            "start": len(results),
        }

        res = self._fetch(f"/feed/updates", params=params)

        data = res.json()

        if (
            len(data["elements"]) == 0
            or (max_results is not None and len(results) >= max_results)
            or (
                max_results is not None
                and len(results) / max_results >= Linkedin._MAX_REPEATED_REQUESTS
            )
        ):
            return results

        results.extend(data["elements"])
        self.logger.debug(f"results grew: {len(results)}")

        return self.get_company_updates(
            public_id=public_id,
            urn_id=urn_id,
            results=results,
            max_results=max_results,
        )

    def get_profile_updates(
        self, public_id=None, urn_id=None, max_results=None, results=None
    ):
        """Fetch profile updates (newsfeed activity) for a given LinkedIn profile.

        :param public_id: LinkedIn public ID for a profile
        :type public_id: str, optional
        :param urn_id: LinkedIn URN ID for a profile
        :type urn_id: str, optional

        :return: List of profile update objects
        :rtype: list
        """

        if results is None:
            results = []

        params = {
            "profileId": {public_id or urn_id},
            "q": "memberShareFeed",
            "moduleKey": "member-share",
            "count": Linkedin._MAX_UPDATE_COUNT,
            "start": len(results),
        }

        res = self._fetch(f"/feed/updates", params=params)

        data = res.json()

        if (
            len(data["elements"]) == 0
            or (max_results is not None and len(results) >= max_results)
            or (
                max_results is not None
                and len(results) / max_results >= Linkedin._MAX_REPEATED_REQUESTS
            )
        ):
            return results

        results.extend(data["elements"])
        self.logger.debug(f"results grew: {len(results)}")

        return self.get_profile_updates(
            public_id=public_id,
            urn_id=urn_id,
            results=results,
            max_results=max_results,
        )

    def get_current_profile_views(self):
        """Get profile view statistics, including chart data.

        :return: Profile view data
        :rtype: dict
        """
        res = self._fetch(f"/identity/wvmpCards")

        data = res.json()

        return data["elements"][0]["value"][
            "com.linkedin.voyager.identity.me.wvmpOverview.WvmpViewersCard"
        ]["insightCards"][0]["value"][
            "com.linkedin.voyager.identity.me.wvmpOverview.WvmpSummaryInsightCard"
        ][
            "numViews"
        ]

    def get_school(self, public_id):
        """Fetch data about a given LinkedIn school.

        :param public_id: LinkedIn public ID for a school
        :type public_id: str

        :return: School data
        :rtype: dict
        """
        params = {
            "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
            "q": "universalName",
            "universalName": public_id,
        }

        res = self._fetch(f"/organization/companies?{urlencode(params)}")

        data = res.json()

        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data))
            return {}

        school = data["elements"][0]

        return school

    def format_date(self,date_dict):
        """
        Formats a date dictionary into separate month, year, and day fields, handling missing or malformed data.
        Returns a tuple of (month, year, day) or ('', '', '') if invalid.
        """
        if not date_dict or not isinstance(date_dict, dict):
            return '', '', ''
        month = date_dict.get('month', '')
        year = date_dict.get('year', '')
        day = date_dict.get('day', '')
        return str(month), str(year), str(day)

    def extract_linkedin_company(self,json_data):
        """
        Extracts detailed LinkedIn company information from JSON data.
        Returns a dictionary with company details, locations, industries, following info, logo, and more.
        """
        result = {
            'company_id': '',
            'name': '',
            'universal_name': '',
            'description': '',
            'tagline': '',
            'industry': [],
            'company_type': '',
            'founded': {
                'year': '',
                'month': '',
                'day': ''
            },
            'staff_count': 0,
            'staff_count_range': {
                'start': None,
                'end': None
            },
            'locations': [],
            'logo': '',
            'company_page_url': '',
            'linkedin_url': '',
            'phone': '',
            'call_to_action': {
                'message': '',
                'type': '',
                'url': ''
            },
            'following_info': {
                'follower_count': 0,
                'following': False
            },
            'entity_urn': '',
            'specialities': [],
            'confirmed_locations': [],
            'headquarter': {},
            'affiliated_companies': []
        }

        included = json_data.get('included', [])

        for item in included:
            item_type = item.get('$type', '')

            # Extract Company Details
            if item_type == 'com.linkedin.voyager.organization.Company':
                result.update({
                    'company_id': self.safe_split_urn(item.get('entityUrn'), default=''),
                    'name': item.get('name', ''),
                    'universal_name': item.get('universalName', ''),
                    'description': item.get('description', ''),
                    'tagline': item.get('tagline', ''),
                    'company_type': item.get('companyType', {}).get('localizedName', ''),
                    'staff_count': item.get('staffCount', 0),
                    'staff_count_range': {
                        'start': item.get('staffCountRange', {}).get('start', None),
                        'end': item.get('staffCountRange', {}).get('end', None)
                    },
                    'company_page_url': item.get('companyPageUrl', ''),
                    'linkedin_url': item.get('url', ''),
                    'phone': item.get('phone', {}).get('number', ''),
                    'entity_urn': item.get('entityUrn', ''),
                    'specialities': item.get('specialities', []),
                    'affiliated_companies': item.get('affiliatedCompanies', [])
                })

                # Extract Founded Date
                founded = item.get('foundedOn', {})
                month, year, day = self.format_date(founded)
                result['founded'] = {
                    'year': year,
                    'month': month,
                    'day': day
                }

                # Extract Call to Action
                call_to_action = item.get('callToAction', {})
                result['call_to_action'] = {
                    'message': call_to_action.get('callToActionMessage', {}).get('text', ''),
                    'type': call_to_action.get('callToActionType', ''),
                    'url': call_to_action.get('url', '')
                }

                # Extract Logo
                logo = item.get('logo', {}).get('image', {})
                root_url = logo.get('rootUrl', '')
                artifacts = logo.get('artifacts', [])
                if artifacts:
                    largest = max(artifacts, key=lambda x: x.get('width', 0))
                    result['logo'] = root_url + largest['fileIdentifyingUrlPathSegment']

                # Extract Headquarter
                headquarter = item.get('headquarter', {})
                result['headquarter'] = {
                    'city': headquarter.get('city', ''),
                    'country': headquarter.get('country', ''),
                    'geographic_area': headquarter.get('geographicArea', ''),
                    'line1': headquarter.get('line1', ''),
                    'line2': headquarter.get('line2', '')
                }

                # Extract Confirmed Locations
                confirmed_locations = item.get('confirmedLocations', [])
                for loc in confirmed_locations:
                    location = {
                        'city': loc.get('city', ''),
                        'country': loc.get('country', ''),
                        'geographic_area': loc.get('geographicArea', ''),
                        'description': loc.get('description', ''),
                        'line1': loc.get('line1', ''),
                        'line2': loc.get('line2', ''),
                        'headquarter': loc.get('headquarter', False),
                        'street_address_opt_out': loc.get('streetAddressOptOut', False)
                    }
                    result['confirmed_locations'].append(location)

            # Extract Industry
            elif item_type == 'com.linkedin.voyager.common.Industry':
                industry = {
                    'entity_urn': item.get('entityUrn', ''),
                    'name': item.get('localizedName', ''),
                    'industry_id': self.safe_split_urn(item.get('entityUrn'), default='')
                }
                result['industry'].append(industry)

            # Extract Following Info
            elif item_type == 'com.linkedin.voyager.common.FollowingInfo':
                result['following_info'] = {
                    'follower_count': item.get('followerCount', 0),
                    'following': item.get('following', False),
                    'entity_urn': item.get('entityUrn', ''),
                    'dash_following_state_urn': item.get('dashFollowingStateUrn', '')
                }

        return result
    def get_company(self, public_id):
        """Fetch data about a given LinkedIn company.

        :param public_id: LinkedIn public ID for a company
        :type public_id: str

        :return: Company data
        :rtype: dict
        """
        params = {
            "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
            "q": "universalName",
            "universalName": public_id,
        }

        res = self._fetch(f"/organization/companies", params=params)

        data = res.json()

        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data["message"]))
            return {}

        company = self.extract_linkedin_company(data)

        return company

    def follow_company(self, following_state_urn, following=True):
        """Follow a company from its ID.

        :param following_state_urn: LinkedIn State URN to append to URL to follow the company
        :type following_state_urn: str
        :param following: The following state to set. True by default for following the company
        :type following: bool, optional

        :return: Error state. If True, an error occured.
        :rtype: boolean
        """
        payload = json.dumps({"patch": {"$set": {"following": following}}})

        res = self._post(
            f"/feed/dash/followingStates/{following_state_urn}", data=payload
        )

        return res.status_code != 200

    def get_conversation_details(self, profile_urn_id):
        """Fetch conversation (message thread) details for a given LinkedIn profile.

        :param profile_urn_id: LinkedIn URN ID for a profile
        :type profile_urn_id: str

        :return: Conversation data
        :rtype: dict
        """
        # passing `params` doesn't work properly, think it's to do with List().
        # Might be a bug in `requests`?
        res = self._fetch(
            f"/messaging/conversations?\
            keyVersion=LEGACY_INBOX&q=participants&recipients=List({profile_urn_id})"
        )

        data = res.json()

        if data["elements"] == []:
            return {}

        item = data["elements"][0]
        item["id"] = get_id_from_urn(item["entityUrn"])

        return item

    def get_conversations(self):
        """Fetch list of conversations the user is in.

        :return: List of conversations
        :rtype: list
        """
        params = {"keyVersion": "LEGACY_INBOX"}

        res = self._fetch(f"/messaging/conversations", params=params)

        return res.json()

    def get_conversation(self, conversation_urn_id: str):
        """Fetch data about a given conversation.

        :param conversation_urn_id: LinkedIn URN ID for a conversation
        :type conversation_urn_id: str

        :return: Conversation data
        :rtype: dict
        """
        res = self._fetch(f"/messaging/conversations/{conversation_urn_id}/events")

        return res.json()

    def create_premium_inmail_payload(
            self,
            subject: str,
            message_body: str,
            mailbox_urn: str,
            recipient_urn: str
    ) -> dict:
        """Create payload for sending a Premium InMail."""
        payload = {
            "message": {
                "body": {
                    "attributes": [],
                    "text": message_body
                },
                "subject": subject,
                "originToken": str(uuid.uuid4()),
                "renderContentUnions": []
            },
            "mailboxUrn": mailbox_urn,
            "trackingId": generate_trackingId_as_charString(),
            "dedupeByClientGeneratedToken": False,
            "hostRecipientUrns": [recipient_urn],
            "hostMessageCreateContent": {
                "com.linkedin.voyager.dash.messaging.MessageCreateContent": {
                    "messageCreateContentUnion": {
                        "premiumInMail": {}
                    }
                }
            }
        }
        return payload
    def send_message(
            self,
            message_body: str,
            conversation_urn_id: Optional[str] = None,
            recipients: Optional[List[str]] = None,
    ) -> bool:
        """Send a message to a given conversation or create a new conversation with recipients.

        :param message_body: Message text to send
        :param conversation_urn_id: LinkedIn URN ID for a conversation
        :param recipients: List of recipient MiniProfile URNs
        :return: True if an error occurred, False if successful
        """
        params = {"action": "create"}

        if not (conversation_urn_id or recipients):
            self.logger.debug("Must provide either [conversation_urn_id] or [recipients].")
            return True

        tracking_id = generate_trackingId_as_charString()

        if conversation_urn_id and not recipients:
            # Sending a message to an existing conversation
            message_event = {
                "eventCreate": {
                    "originToken": str(uuid.uuid4()),
                    "value": {
                        "com.linkedin.voyager.messaging.create.MessageCreate": {
                            "attributedBody": {
                                "text": message_body,
                                "attributes": [],
                            },
                            "attachments": [],
                        }
                    },
                    "trackingId": tracking_id,
                },
                "dedupeByClientGeneratedToken": False,
            }
            res = self._post(
                f"/messaging/conversations/{conversation_urn_id}/events",
                params=params,
                data=json.dumps(message_event),
            )
        elif recipients and not conversation_urn_id:
            message_event=self.create_premium_inmail_payload("Hi",message_body,"urn:li:fsd_profile:ACoAADgAUhwBhrHKTZKVOWd3sxWPce2Ya0tlM6g","urn:li:fsd_profile:ACoAAABD5aEBszJsXSmytpYbhb45EzxCa26d1YI")

            # message_event = {
            #     "eventCreate": {
            #         "originToken": str(uuid.uuid4()),
            #         "value": {
            #             "com.linkedin.voyager.messaging.create.MessageCreate": {
            #                 "messageCreateContentUnion":{
            #                     "premiumInMail": {}
            #                 },
            #                 "attributedBody": {
            #                     "text": message_body,
            #                     "attributes": [],
            #                 },
            #                 "attachments": [],
            #             }
            #         },
            #         "trackingId": tracking_id,
            #     },
            #     "recipients": recipients[0],
            #     "subtype": "MEMBER_TO_MEMBER_INMAIL",  # ✅ Correct subtype
            #     "inmailType": "MEMBER_TO_MEMBER",
            #     "inmailService": "FREE",  # or "PREMIUM" if paid credits
            # }
            payload = message_event
            res = self._post(
                "/voyagerMessagingDashMessengerMessages/",
                params=params,
                data=json.dumps(payload),
            )
            print(res.content)

        return res.status_code != 201

    def mark_conversation_as_seen(self, conversation_urn_id: str):
        """Send 'seen' to a given conversation.

        :param conversation_urn_id: LinkedIn URN ID for a conversation
        :type conversation_urn_id: str

        :return: Error state. If True, an error occured.
        :rtype: boolean
        """
        payload = json.dumps({"patch": {"$set": {"read": True}}})

        res = self._post(
            f"/messaging/conversations/{conversation_urn_id}", data=payload
        )

        return res.status_code != 200

    def get_user_profile(self, use_cache=True) -> Dict:
        """Get the current user profile. If not cached, a network request will be fired.

        :return: Profile data for currently logged in user
        :rtype: dict
        """
        me_profile = self.client.metadata.get("me", {})
        if not self.client.metadata.get("me") or not use_cache:
            res = self._fetch(f"/me")
            me_profile = res.json()
            # cache profile
            self.client.metadata["me"] = me_profile

        return me_profile

    def get_invitations(self, start=0, limit=3):
        """Fetch connection invitations for the currently logged in user.

        :param start: How much to offset results by
        :type start: int
        :param limit: Maximum amount of invitations to return
        :type limit: int

        :return: List of invitation objects
        :rtype: list
        """
        params = {
            "start": start,
            "count": limit,
            "includeInsights": True,
            "q": "receivedInvitation",
        }

        res = self._fetch(
            "/relationships/invitationViews",
            params=params,
        )

        if res.status_code != 200:
            return []

        response_payload = res.json()
        return [element["invitation"] for element in response_payload["elements"]]

    def reply_invitation(
        self, invitation_entity_urn: str, invitation_shared_secret: str, action="accept"
    ):
        """Respond to a connection invitation. By default, accept the invitation.

        :param invitation_entity_urn: URN ID of the invitation
        :type invitation_entity_urn: int
        :param invitation_shared_secret: Shared secret of invitation
        :type invitation_shared_secret: str
        :param action: "accept" or "reject". Defaults to "accept"
        :type action: str, optional

        :return: Success state. True if successful
        :rtype: boolean
        """
        invitation_id = get_id_from_urn(invitation_entity_urn)
        params = {"action": action}
        payload = json.dumps(
            {
                "invitationId": invitation_id,
                "invitationSharedSecret": invitation_shared_secret,
                "isGenericInvitation": False,
            }
        )

        res = self._post(
            f"/relationships/invitations/{invitation_id}",
            params=params,
            data=payload,
        )

        return res.status_code == 200

    def add_connection(self, profile_public_id: str, message="", profile_urn=None):
        """Add a given profile id as a connection.

        :param profile_public_id: public ID of a LinkedIn profile
        :type profile_public_id: str
        :param message: message to send along with connection request
        :type profile_urn: str, optional
        :param profile_urn: member URN for the given LinkedIn profile
        :type profile_urn: str, optional

        :return: Error state. True if error occurred
        :rtype: boolean
        """

        # Validating message length (max size is 300 characters)
        if len(message) > 300:
            self.logger.info("Message too long. Max size is 300 characters")
            return False

        if not profile_urn:
            profile_urn_string = self.get_profile(public_id=profile_public_id)[
                "profile_urn"
            ]
            # Returns string of the form 'urn:li:fs_miniProfile:ACoAACX1hoMBvWqTY21JGe0z91mnmjmLy9Wen4w'
            # We extract the last part of the string
            profile_urn = profile_urn_string.split(":")[-1]

        payload = {
            "invitee": {
                "inviteeUnion": {"memberProfile": f"urn:li:fsd_profile:{profile_urn}"}
            },
            "customMessage": message,
        }
        params = {
            "action": "verifyQuotaAndCreateV2",
            "decorationId": "com.linkedin.voyager.dash.deco.relationships.InvitationCreationResultWithInvitee-2",
        }

        res = self._post(
            "/voyagerRelationshipsDashMemberRelationships",
            data=json.dumps(payload),
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
            params=params,
        )
        # Check for connection_response.status_code == 400 and connection_response.json().get('data', {}).get('code') == 'CANT_RESEND_YET'
        # If above condition is True then request has been already sent, (might be pending or already connected)
        if res.ok:
            return False
        else:
            return True

    def remove_connection(self, public_profile_id: str):
        """Remove a given profile as a connection.

        :param public_profile_id: public ID of a LinkedIn profile
        :type public_profile_id: str

        :return: Error state. True if error occurred
        :rtype: boolean
        """
        res = self._post(
            f"/identity/profiles/{public_profile_id}/profileActions?action=disconnect",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
        )

        return res.status_code != 200

    def track(self, eventBody, eventInfo):
        payload = {"eventBody": eventBody, "eventInfo": eventInfo}
        res = self._post(
            "/li/track",
            base_request=True,
            headers={
                "accept": "*/*",
                "content-type": "text/plain;charset=UTF-8",
            },
            data=json.dumps(payload),
        )

        return res.status_code != 200

    def get_profile_privacy_settings(self, public_profile_id: str):
        """Fetch privacy settings for a given LinkedIn profile.

        :param public_profile_id: public ID of a LinkedIn profile
        :type public_profile_id: str

        :return: Privacy settings data
        :rtype: dict
        """
        res = self._fetch(
            f"/identity/profiles/{public_profile_id}/privacySettings",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
        )
        if res.status_code != 200:
            return {}

        data = res.json()
        return data.get("data", {})

    def get_profile_member_badges(self, public_profile_id: str):
        """Fetch badges for a given LinkedIn profile.

        :param public_profile_id: public ID of a LinkedIn profile
        :type public_profile_id: str

        :return: Badges data
        :rtype: dict
        """
        res = self._fetch(
            f"/identity/profiles/{public_profile_id}/memberBadges",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
        )
        if res.status_code != 200:
            return {}

        data = res.json()
        return data.get("data", {})

    def get_profile_network_info(self, public_profile_id: str):
        """Fetch network information for a given LinkedIn profile.

        Network information includes the following:
        - number of connections
        - number of followers
        - if the account is followable
        - the network distance between the API session user and the profile
        - if the API session user is following the profile

        :param public_profile_id: public ID of a LinkedIn profile
        :type public_profile_id: str

        :return: Network data
        :rtype: dict
        """
        res = self._fetch(
            f"/identity/profiles/{public_profile_id}/networkinfo",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
        )
        if res.status_code != 200:
            return {}

        data = res.json()
        return data.get("data", {})

    def unfollow_entity(self, urn_id: str):
        """Unfollow a given entity.

        :param urn_id: URN ID of entity to unfollow
        :type urn_id: str

        :return: Error state. Returns True if error occurred
        :rtype: boolean
        """
        payload = {"urn": f"urn:li:fs_followingInfo:{urn_id}"}
        res = self._post(
            "/feed/follows?action=unfollowByEntityUrn",
            headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
            data=json.dumps(payload),
        )

        err = False
        if res.status_code != 200:
            err = True

        return err

    def _get_list_feed_posts_and_list_feed_urns(
        self, limit=-1, offset=0, exclude_promoted_posts=True
    ):
        """Get a list of URNs from feed sorted by 'Recent' and a list of yet
        unsorted posts, each one of them containing a dict per post.

        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional
        :param offset: Index to start searching from
        :type offset: int, optional
        :param exclude_promoted_posts: Exclude from the output promoted posts
        :type exclude_promoted_posts: bool, optional

        :return: List of posts and list of URNs
        :rtype: (list, list)
        """
        _PROMOTED_STRING = "Promoted"
        _PROFILE_URL = f"{self.client.LINKEDIN_BASE_URL}/in/"

        l_posts = []
        l_urns = []

        # If count>100 API will return HTTP 400
        count = Linkedin._MAX_UPDATE_COUNT
        if limit == -1:
            limit = Linkedin._MAX_UPDATE_COUNT

        # 'l_urns' equivalent to other functions 'results' variable
        l_urns = []

        while True:
            # when we're close to the limit, only fetch what we need to
            if limit > -1 and limit - len(l_urns) < count:
                count = limit - len(l_urns)
            params = {
                "count": str(count),
                "q": "chronFeed",
                "start": len(l_urns) + offset,
            }
            res = self._fetch(
                f"/feed/updatesV2",
                params=params,
                headers={"accept": "application/vnd.linkedin.normalized+json+2.1"},
            )
            """
            Response includes two keya:
            - ['Data']['*elements']. It includes the posts URNs always
            properly sorted as 'Recent', including yet sponsored posts. The
            downside is that fetching one by one the posts is slower. We will
            save the URNs to later on build a sorted list of posts purging
            promotions
            - ['included']. List with all the posts attributes, but not sorted as
            'Recent' and including promoted posts
            """
            l_raw_posts = res.json().get("included", {})
            l_raw_urns = res.json().get("data", {}).get("*elements", [])

            l_new_posts = parse_list_raw_posts(
                l_raw_posts, self.client.LINKEDIN_BASE_URL
            )
            l_posts.extend(l_new_posts)

            l_urns.extend(parse_list_raw_urns(l_raw_urns))

            # break the loop if we're done searching
            # NOTE: we could also check for the `total` returned in the response.
            # This is in data["data"]["paging"]["total"]
            if (
                (limit > -1 and len(l_urns) >= limit)  # if our results exceed set limit
                or len(l_urns) / count >= Linkedin._MAX_REPEATED_REQUESTS
            ) or len(l_raw_urns) == 0:
                break

            self.logger.debug(f"results grew to {len(l_urns)}")

        return l_posts, l_urns

    def get_feed_posts(self, limit=-1, offset=0, exclude_promoted_posts=True):
        """Get a list of URNs from feed sorted by 'Recent'

        :param limit: Maximum length of the returned list, defaults to -1 (no limit)
        :type limit: int, optional
        :param offset: Index to start searching from
        :type offset: int, optional
        :param exclude_promoted_posts: Exclude from the output promoted posts
        :type exclude_promoted_posts: bool, optional

        :return: List of URNs
        :rtype: list
        """
        l_posts, l_urns = self._get_list_feed_posts_and_list_feed_urns(
            limit, offset, exclude_promoted_posts
        )
        return get_list_posts_sorted_without_promoted(l_urns, l_posts)

    def get_job(self, job_id: str) -> Dict:
        """Fetch data about a given job.
        :param job_id: LinkedIn job ID
        :type job_id: str

        :return: Job data
        :rtype: dict
        """
        params = {
            "decorationId": "com.linkedin.voyager.deco.jobs.web.shared.WebLightJobPosting-23",
        }

        res = self._fetch(f"/jobs/jobPostings/{job_id}", params=params)

        data = res.json()

        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data["message"]))
            return {}

        return data

    def get_post_reactions(self, urn_id, max_results=None, results=None):
        """Fetch social reactions for a given LinkedIn post.

        :param urn_id: LinkedIn URN ID for a post
        :type urn_id: str
        :param max_results: Maximum results to return
        :type max_results: int, optional

        :return: List of social reactions
        :rtype: list

        # Note: This may need to be updated to GraphQL in the future, see https://github.com/tomquirk/linkedin-api/pull/309
        """

        if results is None:
            results = []

        params = {
            "decorationId": "com.linkedin.voyager.dash.deco.social.ReactionsByTypeWithProfileActions-13",
            "count": 10,
            "q": "reactionType",
            "start": len(results),
            "threadUrn": urn_id,
        }

        res = self._fetch("/voyagerSocialDashReactions", params=params)

        data = res.json()

        if (
            len(data["elements"]) == 0
            or (max_results is not None and len(results) >= max_results)
            or (
                max_results is not None
                and len(results) / max_results >= Linkedin._MAX_REPEATED_REQUESTS
            )
        ):
            return results

        results.extend(data["elements"])
        self.logger.debug(f"results grew: {len(results)}")

        return self.get_post_reactions(
            urn_id=urn_id,
            results=results,
            max_results=max_results,
        )

    def react_to_post(self, post_urn_id, reaction_type="LIKE"):
        """React to a given post.
        :param post_urn_id: LinkedIn Post URN ID
        :type post_urn_id: str
        :param reactionType: LinkedIn reaction type, defaults to "LIKE", can be "LIKE", "PRAISE", "APPRECIATION", "EMPATHY", "INTEREST", "ENTERTAINMENT"
        :type reactionType: str

        :return: Error state. If True, an error occured.
        :rtype: boolean
        """
        params = {"threadUrn": f"urn:li:activity:{post_urn_id}"}
        payload = {"reactionType": reaction_type}

        res = self._post(
            "/voyagerSocialDashReactions",
            params=params,
            data=json.dumps(payload),
        )

        return res.status_code != 201

    def get_job_skills(self, job_id: str) -> Dict:
        """Fetch skills associated with a given job.
        :param job_id: LinkedIn job ID
        :type job_id: str

        :return: Job skills
        :rtype: dict
        """
        params = {
            "decorationId": "com.linkedin.voyager.dash.deco.assessments.FullJobSkillMatchInsight-17",
        }
        # https://www.linkedin.com/voyager/api/voyagerAssessmentsDashJobSkillMatchInsight/urn%3Ali%3Afsd_jobSkillMatchInsight%3A3894460323?decorationId=com.linkedin.voyager.dash.deco.assessments.FullJobSkillMatchInsight-17
        res = self._fetch(
            f"/voyagerAssessmentsDashJobSkillMatchInsight/urn%3Ali%3Afsd_jobSkillMatchInsight%3A{job_id}",
            params=params,
        )
        data = res.json()

        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data.get("message")))
            return {}

        return data
