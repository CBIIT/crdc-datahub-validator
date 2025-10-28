#!/usr/bin/env python3
from bento.common.utils import get_logger
from common.constants import TIER_CONFIG, CDE_API_URL, CDE_CODE, CDE_VERSION, CDE_FULL_NAME, STS_API_ALL_URL, STS_API_ONE_URL, \
        CDE_PERMISSIVE_VALUES, STS_DATA_RESOURCE_CONFIG, STS_DATA_RESOURCE_API, STS_DATA_RESOURCE_FILE, STS_DUMP_CONFIG
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
NCIT_CDE_CONCEPT_CODE = "ncit_concept_code"
NCIT_SYNONYMS = "synonyms"
NCIT_VALUE = "value"

def pull_pv_lists(configs, mongo_dao):
    """
    Pull permissible values and synonyms from STS and save them to the database.
    
    :param configs: Configuration settings for the puller.
    :param mongo_dao: Data access object for MongoDB operations.
    """
    log = get_logger('Permissive values and synonym puller')
    api_client = APIInvoker(configs)
    pv_puller = PVPuller(configs, mongo_dao, api_client)
    # synonym_puller = SynonymPuller(configs, mongo_dao, api_client)
    
    try:
        # pull pv, cde, synonym, concept codes
        pv_puller.pull_cde_pv_synonym_concept_codes()
        # test get CDE by code and version
        # get_pv_by_code_version(configs, log, "12447172", "1.00", mongo_dao)
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
        
    def pull_cde_pv_synonym_concept_codes(self):
        """
        pull cde pv from STS API (CDE_API_URL) and save to db
        """
        resource = self.configs[STS_DATA_RESOURCE_CONFIG] if self.configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_API
        # resource = self.configs[STS_DATA_RESOURCE_CONFIG] if self.configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_FILE
        try:
            cde_records, synonym_records, concept_codes_records = retrieveAllCDEViaAPI(self.configs, self.log, self.api_client) if resource == STS_DATA_RESOURCE_API \
                else retrieveAllCDEViaDumpFile(self.configs, self.log, self.api_client)
            if not cde_records or len(cde_records) == 0:
                self.log.info("No CDE found!")
                return
            self.log.info(f"{len(cde_records)} unique CDE are retrieved!")
            result, msg = self.mongo_dao.upsert_cde(list(cde_records))
            if result: 
                self.log.info(f"CDE PV are pulled and save successfully!")
            else:
                self.log.error(f"Failed to pull and save CDE PV! {msg}")

            if not synonym_records or len(synonym_records) == 0:
                self.log.info("No synonym found!")
                return
            self.log.info(f"{len(synonym_records)} unique synonyms are retrieved!")
            result = self.mongo_dao.insert_synonyms(list(synonym_records))
            if result is not None:
                self.log.info(f"CDE Synonyms are pulled and save successfully!")

            if not concept_codes_records or len(concept_codes_records) == 0:
                self.log.info("No concept code found!")
                return
            self.log.info(f"{len(concept_codes_records)} unique concept codes are retrieved!")
            result = self.mongo_dao.insert_concept_codes(list(concept_codes_records))
            if result is not None:
                self.log.info(f"CDE Concept Codes are pulled and save successfully!")
            self.log.info(f"All CDE PVs, Synonyms and Concept Codes are pulled and saved successfully!")
            return
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to retrieve CDE PVs.")

def retrieveAllCDEViaAPI(configs, log, api_client=None):
    """
    extract cde from cde dump file
    """
    sts_api_url = configs[STS_API_ALL_URL]
    log.info(f"Retrieving cde from {sts_api_url}...")
    if not api_client:
        api_client = APIInvoker(configs)
    results = api_client.get_all_data_elements(sts_api_url)
    if not results or len(results) == 0:
        log.error(f"No cde/pvs retrieve from STS API, {sts_api_url}.")
        return None, None, None
    cde_records, synonym_set, concept_code_set = process_sts_cde_pv(results, log)
    log.info(f"Retrieved CDE PVs from {sts_api_url}.")
    return cde_records, synonym_set, concept_code_set

def retrieveAllCDEViaDumpFile(configs, log, api_client=None):
    """
    extract cde from cde dump file
    """
    sts_file_url = configs[STS_DUMP_CONFIG].format(configs[TIER_CONFIG])
    log.info(f"Retrieving cde from {sts_file_url}...")
    if not api_client:
        api_client = APIInvoker(configs)
    results = api_client.get_synonyms(sts_file_url)
    cde_records, synonym_set, concept_code_set = process_sts_cde_pv(results, log)
    log.info(f"Retrieved CDE PVs from {sts_file_url}.")
    return cde_records, synonym_set, concept_code_set

def process_sts_cde_pv(sts_results, log, cde_only=False):
    """
    get cde pv from sts api
    :param sts_api_url: sts api url
    """
    cde_set = set()
    cde_records = []
    synonym_set = set()
    concept_code_set = set()
    if not sts_results or len(sts_results) == 0:
        log.error(f"No cde/pvs retrieve from STS API.")
        return None, None, None
    cde_list  = [item for item in sts_results if item.get(CDE_CODE) and item.get(CDE_CODE) != 'null'] 
    if not cde_list or len(cde_list) == 0:
        log.error(f"No cde found in STS API results.")
        return None, None, None
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
        if cde_only:
            continue
        # extract synonyms
        if item.get(CDE_PV_NAME) and len(item.get(CDE_PV_NAME)) > 0 and item.get(CDE_PV_NAME)[0].get(NCIT_SYNONYMS):
            compose_synonym_record(item, synonym_set)

        # extract concept codes
        if item.get(CDE_PV_NAME) and len(item.get(CDE_PV_NAME)) > 0 and item.get(CDE_PV_NAME)[0].get(NCIT_CDE_CONCEPT_CODE):
            compose_concept_code_record(item, concept_code_set)

    return cde_records, synonym_set, concept_code_set

def extract_pv_list(cde_pv_list):
    """
    extract pv list from cde dump file
    """
    pv_list = None
    if cde_pv_list and len(cde_pv_list) > 0 and cde_pv_list[0].get(NCIT_VALUE): 
        pv_list = [item.get(NCIT_VALUE) for item in cde_pv_list if item.get(NCIT_VALUE) is not None]
    if cde_pv_list and any(item.get(NCIT_VALUE) for item in cde_pv_list):
        pv_list = [item[NCIT_VALUE] for item in cde_pv_list if NCIT_VALUE in item and item[NCIT_VALUE] is not None]
        contains_http = any(s for s in pv_list if isinstance(s, str) and s.startswith(("http:", "https:")))
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

def compose_synonym_record(cde_item, synonym_set):
    """
    compose synonym record from cde dump file
    """
    pv_list = cde_item.get(CDE_PV_NAME)
    if pv_list:
        for pv_item in pv_list:
            synonyms = pv_item.get(NCIT_SYNONYMS)
            if synonyms:
                for synonym in synonyms:
                        if synonym:
                            synonym_key = (synonym, pv_item.get(NCIT_VALUE))
                            if synonym_key in synonym_set:
                                continue
                            synonym_set.add(synonym_key)
    return

def compose_concept_code_record(cde_item, concept_code_set):
    """
    compose concept code record from cde dump file
    """
    pv_list = cde_item.get(CDE_PV_NAME)
    cde_code = cde_item.get(CDE_CODE)
    if pv_list:
        for pv in pv_list:
            value = pv.get(NCIT_VALUE)
            concept_code = pv.get(NCIT_CDE_CONCEPT_CODE)
            if concept_code:
                concept_code_key = (cde_code, value, concept_code)
                if concept_code_key in concept_code_set:
                    continue
                concept_code_set.add(concept_code_key)
    return

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
        return None
    cde_records = []
    api_client = APIInvoker(configs)
    resource = configs[STS_DATA_RESOURCE_CONFIG] if configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_API
    # resource = configs[STS_DATA_RESOURCE_CONFIG] if configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_FILE
    if resource == STS_DATA_RESOURCE_API:
        sts_api_url = configs[STS_API_ONE_URL]
        if not sts_api_url:
            msg = "STS API url is not configured."
            log.error(f"Invalid STS API URL.")
            return None
        if not cde_version:
            sts_api_url = sts_api_url.replace("/{cde_version}", "")
            sts_api_url = sts_api_url.format(cde_code=cde_code)
            cde_version = None
        else:   
            sts_api_url = sts_api_url.format(cde_code=cde_code, cde_version=cde_version)
        log.info(f"Retrieving cde from {sts_api_url} for {cde_code}/{cde_version}...")
        try:
            results = api_client.get_all_data_elements(sts_api_url)
            cde_records, _, _ = process_sts_cde_pv(results, log, True)
            if not cde_records or len(cde_records) == 0:
                msg = f"No CDE found for {cde_code}/{cde_version}."
                log.info(msg)
                return None
        except Exception as e:
            log.exception(e)
            msg = f"Failed to retrieve CDE PVs for {cde_code}/{cde_version}."
            log.exception(msg)
            return None
        except Exception as e:
            log.exception(e)
            msg = f"Failed to retrieve CDE PVs for {cde_code}/{cde_version}."
            log.exception(msg)
            return None
    else:
        sts_file_url = configs[STS_DUMP_CONFIG].format(configs[TIER_CONFIG])
        log.info(f"Retrieving cde from {sts_file_url} for {cde_code}/{cde_version}...")
        try:
            results = api_client.get_synonyms(sts_file_url)
            cde_records, _, _ = process_sts_cde_pv(results, log, True)

        except Exception as e:
            log.exception(e)
            msg = f"Failed to retrieve CDE PVs for {cde_code}/{cde_version}."
            log.exception(msg)
            return None

    if not cde_records or len(cde_records) == 0:
        msg = f"No CDE found for {cde_code}/{cde_version}."
        log.info(msg)
        return None
    log.info(f"{len(cde_records)} unique CDE are retrieved!")
    cde_record = next((item for item in cde_records if item[CDE_CODE] == cde_code and item[CDE_VERSION] == cde_version), None)
    if not cde_record:
        msg = f"No CDE found for {cde_code}/{cde_version}."
        log.info(msg)
        return None
    if not cde_records or len(cde_records) == 0:
        msg = f"No CDE found for {cde_code}/{cde_version}."
        log.info(msg)
        return None
    log.info(f"{len(cde_records)} unique CDE are retrieved!")
    cde_record = next((item for item in cde_records if item[CDE_CODE] == cde_code and item[CDE_VERSION] == cde_version), None)
    if not cde_record:
        msg = f"No CDE found for {cde_code}/{cde_version}."
        log.info(msg)
        return None
    log.info(f"Retrieved CDE for {cde_code}/{cde_version}.")
    # save cde pv to db
    result, _ = mongo_dao.upsert_cde([cde_record])
    if result:
        log.info(f"CED PV are pulled and save successfully!")
    else:
        log.error(f"Failed to pull and save CDE PV! {msg}")
    return cde_record