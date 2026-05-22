#!/usr/bin/env python3

import requests
import json
from bento.common.utils import get_logger
# from common.constants import UPLOAD_TYPE, API_URL, SUBMISSION_ID, TOKEN
from common.utils import get_exception_msg

class APIInvoker:
    def __init__(self, configs):
        self.log = get_logger('GraphQL API')
        self.configs = configs

    #1) get sts temp credential for file/metadata uploading to S3 bucket
    def get_temp_credential(self):
        self.cred = None
        body = f"""
        mutation {{
            createTempCredentials (submissionID: \"{self.submissionId}\") {{
                accessKeyId,
                secretAccessKey,
                sessionToken
            }}
        }}
        """
        try:
            response = requests.post(url=self.url, headers=self.headers, json={"query": body})
            status = response.status_code
            self.log.info(f"get_temp_credential response status code: {status}.")
            if status == 200: 
                results = response.json()
                if results.get("errors"):
                    self.log.error(f'Retrieve temporary credential failed - {results.get("errors")[0].get("message")}.')  
                    return False
                else:
                    self.cred = results.get("data").get("createTempCredentials")
                    return True  
            else:
                self.log.error(f'Retrieve temporary credential failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return False

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Retrieve temporary credential failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return False


    def get_data_element_by_cde_code(self, cde_code, api_uri, version=None):
        """
        Retrieve data element by cde code
        :param cde_code: cde code
        :param api_uri: api uri
        :param version: version
        :return: data element

        """
        url = api_uri + str(cde_code) + ("" if version is None else "?version=" + str(version))
        headers = {
            "accept": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            status = response.status_code
            # self.log.info(f"get_data_element_by_cde_code response status code: {status}.")
            if status == 200:
                results = response.json()
                if results.get("errors"):
                    self.log.error(f'Retrieve data element by cde code failed - {results.get("errors")[0].get("message")}.')
                    return None
                else:
                    return results
            else:
                self.log.error(f'Retrieve data element by cde code failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return None

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Retrieve data element by cde code failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return None
        
    def get_all_data_elements(self, api_uri):
        """
        Retrieve data elements
        :param api_uri: api uri
        :return: data elements
        """
        headers = {
            "accept": "application/json"
        }
        try:
            # response = requests.get(api_uri, headers=headers)
            response = requests.get(api_uri, headers=headers, verify=False)
            status = response.status_code
            # self.log.info(f"get_data_element_by_cde_code response status code: {status}.")
            if status == 200:
                results = response.json()
                if isinstance(results, dict) and "errors" in results:
                    self.log.error(f'Retrieve data element by cde code failed - {results.get("errors")[0].get("message")}.')
                    return None
                else:
                    return results
            else:
                self.log.error(f'Retrieve data element by cde code failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                return None

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Retrieve data element by cde code failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return None

    def get_all_data_elements_v2(self, api_uri_list):
        """
        Retrieve data elements
        :param api_uri: api uri
        :return: data elements
        """
        headers = {
            "accept": "application/json"
        }
        try:
            # response = requests.get(api_uri, headers=headers)
            results_list = []
            for api_uri in api_uri_list:
                response = requests.get(api_uri, headers=headers, verify=False)
                status = response.status_code
                # self.log.info(f"get_data_element_by_cde_code response status code: {status}.")
                if status == 200:
                    results = response.json()
                    if isinstance(results, dict) and "errors" in results:
                        self.log.error(f'Retrieve data element by cde code failed - {results.get("errors")[0].get("message")}.')
                        return None
                    else:
                        results_list.append(results)
                else:
                    self.log.error(f'Retrieve data element by cde code failed (code: {status}) - internal error. Please try again and contact the helpdesk if this error persists.')
                    #return None
                    results_list.append(None)
            return results_list

        except Exception as e:
            self.log.debug(e)
            self.log.exception(f'Retrieve data element by cde code failed - internal error. Please try again and contact the helpdesk if this error persists.')
            return None
        
    def list_github_files(self, url, branch, token=None):
        headers = {}
        if token:
            headers["Authorization"] = f"token {token}"
        try:
            params = {"ref": branch} 
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException as e:
            self.log.debug(e)
            self.log.exception(f"Error retrieving synonyms: {e}")
            return None
        except json.JSONDecodeError as e:
            self.log.debug(e)
            self.log.exception(f"Error decoding JSON response: {e}")
            return None
        
    def get_synonyms(self, api_url):
        """
        Retrieve synonyms from the API
        :param api_url: API URL
        :return: List of synonyms
        """
        headers = {
            "accept": "application/json"
        }
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e:
            self.log.debug(e)
            self.log.exception(f"Error retrieving synonyms: {e}")
            return None
        except json.JSONDecodeError as e:
            self.log.debug(e)
            self.log.exception(f"Error decoding JSON response: {e}")
            return None
