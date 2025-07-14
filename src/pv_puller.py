#!/usr/bin/env python3
from bento.common.utils import get_logger
from common.constants import TIER_CONFIG, CDE_API_URL, CDE_CODE, CDE_VERSION, CDE_FULL_NAME, STS_API_ALL_URL, STS_API_ONE_URL, \
        CDE_PERMISSIVE_VALUES
from common.utils import get_exception_msg
from common.api_client import APIInvoker

MODEL_DEFS = "models"
CADSR_DATA_ELEMENT = "DataElement"
CADSR_VALUE_DOMAIN = "ValueDomain"
CADSR_DATA_ELEMENT_LONG_NAME = "longName"
CADSR_PERMISSIVE_VALUES = "PermissibleValues"
FILE_DOWNLOAD_URL = "download_url"
FILE_NAME = "name"
FILE_TYPE = "type"
CDE_PV_NAME = "permissibleValues"
STS_FILE_URL = "https://raw.githubusercontent.com/CBIIT/crdc-datahub-terms/{}/mdb_pvs_synonyms.json"

def pull_pv_lists(configs, mongo_dao):
    """
    Pull permissible values and synonyms from STS and save them to the database.
    
    :param configs: Configuration settings for the puller.
    :param mongo_dao: Data access object for MongoDB operations.
    """
    log = get_logger('Permissive values and synonym puller')
    api_client = APIInvoker(configs)
    pv_puller = PVPuller(configs, mongo_dao, api_client)
    synonym_puller = SynonymPuller(configs, mongo_dao, api_client)
    
    try:
        # pull pv
        pv_puller.pull_cde_pv()
        # pull synonyms
        synonym_puller.pull_synonyms()
    except (KeyboardInterrupt, SystemExit):
        print("Task is stopped...")
    except Exception as e:
        log.critical(e)
        log.critical(
            f'Something wrong happened while pulling permissive values! Check debug log for details.')

class PVPuller:
    """
    Class for pulling permissible values from STS and saving them to the database.
    """
    def __init__(self, configs, mongo_dao, api_client):
        self.log = get_logger('Permissive values puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        self.api_client = api_client
        
    def pull_cde_pv(self):
        """
        pull cde pv from STS API (CDE_API_URL) and save to db
        """
        try:
            cde_records = retrieveAllCDE(self.configs, self.log, self.api_client)
            if not cde_records or len(cde_records) == 0:
                self.log.info("No cde found!")
                return
            self.log.info(f"{len(cde_records)} unique CDE are retrieved!")
            result, msg = self.mongo_dao.upsert_cde(list(cde_records))
            if result: 
                self.log.info(f"CED PV are pulled and save successfully!")
            else:
                self.log.error(f"Failed to pull and save CDE PV! {msg}")
            return
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to retrieve CDE PVs.")

def retrieveAllCDE(configs, log, api_client=None):
    """
    extract cde from cde dump file
    """
    sts_api_url = configs[STS_API_ALL_URL]
    log.info(f"Retrieving cde from {sts_api_url}...")
    if not api_client:
        api_client = APIInvoker(configs)
    results = api_client.get_all_data_elements(sts_api_url)
    cde_records = process_sts_cde_pv(results, log)
    log.info(f"Retrieved CDE PVs from {sts_api_url}.")
    return cde_records

def process_sts_cde_pv(sts_results, log):
    """
    get cde pv from sts api
    :param sts_api_url: sts api url
    """
    cde_set = set()
    cde_records = []
    if not sts_results or len(sts_results) == 0:
        log.error(f"No cde/pvs retrieve from STS API.")
        return None
    cde_list  = [item for item in sts_results if item.get(CDE_CODE) and item.get(CDE_CODE) != 'null'] 
    if not cde_list or len(cde_list) == 0:
        log.error(f"No cde found in STS API results.")
        return None 
    for item in cde_list:
        code = item.get(CDE_CODE)
        version = item.get(CDE_VERSION) if item.get(CDE_VERSION) and item.get(CDE_VERSION) != 'null' else None
        cde_key = (code, version)
        if cde_key in cde_set:
            continue
        cde_set.add(cde_key)
        cde_record = compose_cde_record(item)
        cde_records.append(
            cde_record
        )
    return cde_records

def extract_pv_list(cde_pv_list):
    """
    extract pv list from cde dump file
    """
    pv_list = None
    if cde_pv_list and len(cde_pv_list) > 0 and cde_pv_list[0].get('value'): 
        pv_list  = [item['value'] for item in cde_pv_list] 
        contains_http = any(s for s in pv_list if s.startswith(("http:", "https:")))
        if contains_http:
            return None
        # strip white space if the value is a string
        if pv_list and isinstance(pv_list[0], str): 
            pv_list = [item.strip() for item in pv_list]
    
    return pv_list

def compose_cde_record(cde_item):
    """
    compose cde record from cde dump file
    """
    cde_record = {
        CDE_FULL_NAME: cde_item.get(CDE_FULL_NAME),
        CDE_CODE: cde_item.get(CDE_CODE),
        CDE_VERSION: cde_item.get(CDE_VERSION) if cde_item.get(CDE_VERSION) and cde_item.get(CDE_VERSION) != 'null' else None,
        CDE_PERMISSIVE_VALUES: extract_pv_list(cde_item.get(CDE_PV_NAME))
    }
    return cde_record

def get_pv_by_code_version(configs, log, cde_code, cde_version, mongo_dao):
    """
    get permissive values by cde code and version in real time
    :param cde_code: cde code
    :param cde_version: cde version
    """
    msg = None
    if cde_code is None:
        msg = "CDE code is required."
        log.error(f"Invalid CDE code.")
        return None, msg
    cde_records = []
    api_client = APIInvoker(configs)
    sts_api_url = configs[STS_API_ONE_URL]
    if not sts_api_url:
        msg = "STS API url is not configured."
        log.error(f"Invalid STS API URL.")
        return None, msg
    if not cde_version:
        sts_api_url = sts_api_url.replace("/{cde_version}", "")
        sts_api_url = sts_api_url.format(cde_code=cde_code)
        cde_version = None
    else:
        sts_api_url = sts_api_url.format(cde_code=cde_code, cde_version=cde_version)
    log.info(f"Retrieving cde from {sts_api_url} for {cde_code}/{cde_version}...")
    try:
        results = api_client.get_all_data_elements(sts_api_url)
        cde_records = process_sts_cde_pv(results, log)
        if not cde_records or len(cde_records) == 0:
            msg = f"No CDE found for {cde_code}/{cde_version}."
            log.info(msg)
            return None, msg
        log.info(f"{len(cde_records)} unique CDE are retrieved!")
        cde_record = next((item for item in cde_records if item[CDE_CODE] == cde_code and item[CDE_VERSION] == cde_version), None)
        return cde_record
    except Exception as e:
        log.exception(e)
        msg = f"Failed to retrieve CDE PVs for {cde_code}/{cde_version}."
        log.exception(msg)
        return None, msg

def get_pv_by_datacommon_version_cde(tier, data_commons, data_model_version, cde_code, cde_version, log, mongo_dao):
    """
    extracts the CDE list by data_commons, data_model_version and finds matches based on the provided CDE code and version,
    finally saves the CDE list into DB.

    :param tier: The tier of the environment (e.g., 'dev', 'qa', 'prod').
    :param data_commons: The name of the data commons.
    :param data_model_version: The version of the data model.
    :param cde_code: The code of the CDE to retrieve.
    :param cde_version: The version of the CDE to retrieve.
    :param log: Logger instance for logging information and errors.
    :param mongo_dao: Data access object for MongoDB operations.
    :returns: A dictionary containing the CDE full name, code, version, and permissive values, or None if the CDE is not found or an error occurs.
    """
    
    # construct the sts dump file url based on tier
    sts_file_url = STS_FILE_URL.format(tier)
    try:
        log.info(f"Extracting cde from {sts_file_url}")
        api_client = APIInvoker({})
        result = api_client.get_synonyms(sts_file_url)
        if not result or len(result) == 0:
            log.error(f"CDE dump file,{sts_file_url} is not found! ")
            return None
        cde_list  = [item for item in result if item.get(CDE_CODE) and item.get(CDE_CODE) != 'null'] 
        if not cde_list or len(cde_list) == 0:
            log.error(f"No cde found in {sts_file_url}")
            return
        
        cde_set = set()
        cde_records = []
        return_cde = None
        for item in cde_list:
            code = item.get(CDE_CODE)
            version = item.get(CDE_VERSION) if item.get(CDE_VERSION) and item.get(CDE_VERSION) != 'null' else None
            cde_key = (code, version)
            if cde_key in cde_set:
                continue
            cde_set.add(cde_key)
            cde_record = compose_cde_record(item)
            cde_records.append(
                cde_record
            )
            if code  == cde_code and version == cde_version:
                return_cde = cde_record
        # save all extracted CDEs
        mongo_dao.upsert_cde(cde_records)
        # return matched CDE
        return return_cde
    except Exception as e:
        log.exception(e)
        log.exception(f"Failed to extract cde from {sts_file_url}")
        return None

class SynonymPuller:
    """
    pull synonyms from sts and save to db
    """
    def __init__(self, configs, mongo_dao, api_client):
        self.log = get_logger('Synonyms puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        self.api_client = api_client

    def pull_synonyms(self):
        # init a synonym set to make sure all synonym/pv pairs are unique
        synonym_set = set()
        concept_code_set = set()
        tier = self.configs.get(TIER_CONFIG)
        sts_file_url = STS_FILE_URL.format(tier)
        try: 
            # 1) pull synonyms from the file
            self.get_synonyms_by_datacommon_version(sts_file_url, synonym_set, concept_code_set)
            # 2) save synonyms to db
            if not synonym_set or len(synonym_set) == 0:
                self.log.info("No synonyms found!")
                return
            self.log.info(f"{len(synonym_set)} unique synonym/pv pairs are retrieved!")
            count = self.mongo_dao.insert_synonyms(list(synonym_set))
            self.log.info(f"{count} new synonyms are inserted!")

            # 3) save concept code to db
            if not concept_code_set or len(concept_code_set) == 0:
                self.log.info("No concept code found!")
                return
            self.log.info(f"{len(concept_code_set)} unique concept code/pv pairs are retrieved!")
            count = self.mongo_dao.insert_concept_codes(list(concept_code_set))
            self.log.info(f"{count} concept codes are inserted!")
            
            return

        except Exception as e: #catch all unhandled exception
            self.log.exception(e)
            msg = f"Failed to pull synonyms, {get_exception_msg()}!"
            self.log.exception(msg)
            return False
        
    def get_synonyms_by_datacommon_version(self, synonym_url, synonym_set, concept_code_set):
        """
        get synonyms from dump file url in Github repo
        :param synonym_url
        :param synonym_set
        """
        try:
            self.log.info(f"Pulling synonyms and concept codes from {synonym_url}")
            result = self.api_client.get_synonyms(synonym_url)
            if not result or len(result) == 0:
                self.log.info(f"Synonyms and concept codes for {synonym_url} are not found! ")
                return None
            # filter out empty synonyms
            synonyms  = [item for item in result if item.get(CDE_PV_NAME) and item.get(CDE_PV_NAME)[0].get('synonyms')] 
            for item in synonyms:
                pv_list = item.get(CDE_PV_NAME)
                if pv_list:
                    for pv in pv_list:
                        value = pv.get('value')
                        synonyms_val = pv.get('synonyms')
                        if synonyms:
                            for synonym in synonyms_val:
                                if synonym:
                                    synonym_key = (synonym, value)
                                    if synonym_key in synonym_set:
                                        continue
                                    synonym_set.add(synonym_key)
            # extract concept codes
            self.get_concept_code(result, concept_code_set)
    
        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to pull synonyms from {synonym_url}, {get_exception_msg()}!"
            self.log.exception(msg)
            return None
    
    def get_concept_code(self, result, concept_code_set):
        """
        get synonyms from dump file url in Github repo
        :param synonym_url
        :param synonym_set
        """
        self.log.info(f"Extract concept codes")
        # filter out empty synonyms
        concept_codes  = [item for item in result if item.get(CDE_PV_NAME) and item.get(CDE_PV_NAME)[0].get('ncit_concept_code')] 
        for item in concept_codes:
            pv_list = item.get(CDE_PV_NAME)
            cde_code = item.get(CDE_CODE)
            if pv_list:
                for pv in pv_list:
                    value = pv.get('value')
                    concept_code = pv.get('ncit_concept_code')
                    if concept_code:
                        concept_code_key = (cde_code, value, concept_code)
                        if concept_code_key in concept_code_set:
                            continue
                        concept_code_set.add(concept_code_key)
        return
