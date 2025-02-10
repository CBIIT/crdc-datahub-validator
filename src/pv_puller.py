#!/usr/bin/env python3
from bento.common.utils import get_logger
from common.constants import MODEL_FILE_DIR, TIER_CONFIG, CDE_API_URL, CDE_CODE, CDE_VERSION, CDE_FULL_NAME, ID, CREATED_AT, UPDATED_AT,\
        TERM_CODE, TERM_VERSION, SYNONYM_API_URL, CDE_PERMISSIVE_VALUES
from common.utils import get_exception_msg, current_datetime, get_uuid_str, dump_dict_to_json, dump_dict_to_tsv, get_date_time
from common.pv_term_reader import TermReader
from common.api_client import APIInvoker

MODEL_DEFS = "models"
CADSR_DATA_ELEMENT = "DataElement"
CADSR_VALUE_DOMAIN = "ValueDomain"
CADSR_DATA_ELEMENT_LONG_NAME = "longName"
FILE_DOWNLOAD_URL = "download_url"
FILE_NAME = "name"
FILE_TYPE = "type"

def pull_pv_lists(configs, mongo_dao):
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

"""
pull permissive values from CDE and save to db
"""
class PVPuller:
    
    def __init__(self, configs, mongo_dao, api_client):
        self.log = get_logger('Permissive values puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        self.api_client = api_client
        
    def pull_cde_pv(self):
        """
        pull cde pv from CDE dump files in github and save to CDE collection in DB
        """
        file_list = get_cde_dump_files(self.api_client, self.configs[SYNONYM_API_URL], self.configs[TIER_CONFIG], self.log)
        if not file_list:
            self.log.info("No CDE dump files found!")   
            return
        cde_set = set()
        cde_records = []
        for file in file_list:
            self.extract_cde_from_file(file[FILE_DOWNLOAD_URL], cde_set, cde_records)
        
        if not cde_records or len(cde_records) == 0:
            self.log.info("No cde found!")
            return
        
        self.log.info(f"{len(cde_set)} unique CDE are retrieved!")
        result, msg = self.mongo_dao.upsert_cde(list(cde_records))
        if result: 
            self.log.info(f"CED PV are pulled and save successfully!")
        else:
            self.log.error(f"Failed to pull and save CDE PV! {msg}")
        return

    def extract_cde_from_file(self, download_url, cde_set, cde_record):
        """
        extract cde from file
        """
        try:
            self.log.info(f"Extracting cde from {download_url}")
            result = self.api_client.get_synonyms(download_url)
            if not result or len(result) == 0:
                self.log.info(f"CDE dump files in {download_url} are not found! ")
                return None
            cde_list  = [item for item in result if item.get(CDE_CODE) and item.get(CDE_CODE) != 'null'] 
            if not cde_list or len(cde_list) == 0:
                self.log.info(f"No cde found in {download_url}")
                return
            
            for cde in cde_list:
                cde_code = cde.get(CDE_CODE)
                cde_version = cde.get(CDE_VERSION) if cde.get(CDE_VERSION) and cde.get(CDE_VERSION) != 'null' else None
                cde_key = (cde_code, cde_version)
                if cde_key in cde_set:
                    continue
                cde_set.add(cde_key)
                cde_long_name  = cde.get(CDE_FULL_NAME)
                pv_list = None
                if cde.get('permissibleValues') and cde.get('permissibleValues')[0].get('value'): 
                    pv_list  = [item.get('value') for item in cde['permissibleValues']] 
                    contains_http = any(s for s in pv_list if "http:" in s or "https:" in s )
                    if contains_http:
                        pv_list = None #new requirement in CRDCDH-1723
                
                cde_record.append({
                    CDE_FULL_NAME: cde_long_name,
                    CDE_CODE: cde_code,
                    CDE_VERSION: cde_version,
                    CDE_PERMISSIVE_VALUES: pv_list
                })

        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to extract cde from {download_url}")

def get_cde_dump_files(api_client, github_url, tier, log):
    """
    get cde dump files from github
    """
    try:        
        file_list = api_client.list_github_files(github_url, tier)
        file_list = [f for f in file_list if f[FILE_NAME].endswith("_sts.json")]
        if not file_list or len(file_list) == 0:
            log.info(f"CDE dump files for {tier} are not found! ")
            return None
        return file_list
    except  Exception as e:
        log.exception(e)
        log.exception(f"Failed to get cde dump files from {github_url}")
        return None

def get_pv_by_code_version(configs, log, data_common, prop_name, cde_code, cde_version):
    """
    get permissive values by cde code and version
    :param cde_code: cde code
    :param cde_version: cde version
    """
    msg = None

    # 3) pull new cde permissive values and save to db
    api_client = APIInvoker(configs)
    result = api_client.get_data_element_by_cde_code(cde_code, configs[CDE_API_URL], cde_version)
    if not result or not result.get(CADSR_DATA_ELEMENT) or not result[CADSR_DATA_ELEMENT].get(CADSR_VALUE_DOMAIN):
        log.info(f"No data element found for {data_common}/{prop_name}:{cde_code}:{cde_version}")
        return None, "No CDE element found."
    pv_list = result[CADSR_DATA_ELEMENT][CADSR_VALUE_DOMAIN].get(CDE_PERMISSIVE_VALUES)
    cde_long_name = result[CADSR_DATA_ELEMENT].get(CADSR_DATA_ELEMENT_LONG_NAME)
    if not pv_list or len(pv_list) == 0:
        log.info(f"No permissive values found for {data_common}/{prop_name}:{cde_code}:{cde_version}")
        msg = "No CDE permissive values defined for the CDE code."
        pv_list = []
    else:
        contains_http = any(s for s in pv_list if "http:" in s.get("value") or "https:" in s.get("value") or "http:" in s or "https:" in s )
        if not contains_http:
            pv_list = [ item["value"] for item in pv_list]
        else: 
            pv_list = None #new requirement in CRDCDH-1723
    return {
        ID: get_uuid_str(),
        CDE_FULL_NAME: cde_long_name,
        CDE_CODE: cde_code,
        CDE_VERSION: cde_version,
        CDE_PERMISSIVE_VALUES: pv_list,
        CREATED_AT: current_datetime(),
        UPDATED_AT: current_datetime(),
    }, msg

"""
pull synonyms from sts and save to db
"""
class SynonymPuller:
    def __init__(self, configs, mongo_dao, api_client):
        self.log = get_logger('Synonyms puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        self.api_client = api_client

    def pull_synonyms(self):
        # init a synonym set to make sure all synonym/pv pairs are unique
        synonym_set = set()
        try: 
        #  1) get contents in branch (tier) of the repo 
            synonym_url = str(self.configs[SYNONYM_API_URL])
            file_list = get_cde_dump_files(self.api_client, synonym_url, self.configs[TIER_CONFIG], self.log)
            """ 
            file structure:           
                'name' = 'README.md'
                'path' = 'README.md'
                'sha' = '38d749509c716ac79d3ad2540a50b043c075c0be'
                'size' = 21
                'url' = 'https://api.github.com/repos/CBIIT/crdc-datahub-terms/contents/README.md?ref=dev2'
                'html_url' = 'https://github.com/CBIIT/crdc-datahub-terms/blob/dev2/README.md'
                'git_url' = 'https://api.github.com/repos/CBIIT/crdc-datahub-terms/git/blobs/38d749509c716ac79d3ad2540a50b043c075c0be'
                'download_url' = 'https://raw.githubusercontent.com/CBIIT/crdc-datahub-terms/dev2/README.md'
                'type' = 'file'
            """
            for file in file_list:
                if not (file[FILE_TYPE] == "file" and "_sts.json" in file[FILE_NAME]):
                    continue
                # 2) pull synonyms from the file
                self.get_synonyms_by_datacommon_version(file[FILE_DOWNLOAD_URL], synonym_set)

        #  3) save synonyms to db
            if not synonym_set or len(synonym_set) == 0:
                self.log.info("No synonyms found!")
                return
            self.log.info(f"{len(synonym_set)} unique synonym/pv pairs are retrieved!")
            count = self.mongo_dao.insert_synonyms(list(synonym_set))
            self.log.info(f"{count} new synonyms are inserted!")
            return

        except Exception as e: #catch all unhandled exception
            self.log.exception(e)
            msg = f"Failed to pull synonyms, {get_exception_msg()}!"
            self.log.exception(msg)
            return False
        
    def get_synonyms_by_datacommon_version(self, synonym_url, synonym_set):
        """
        get synonyms from dump file url in Github repo
        :param synonym_url
        :param synonym_set
        """
        try:
            self.log.info(f"Pulling synonyms from {synonym_url}")
            result = self.api_client.get_synonyms(synonym_url)
            if not result or len(result) == 0:
                self.log.info(f"Synonyms for {synonym_url} are not found! ")
                return None
            # filter out empty synonyms
            synonyms  = [item for item in result if item.get('permissibleValues') and item.get('permissibleValues')[0].get('synonyms')] 
            for item in synonyms:
                pv_list = item.get('permissibleValues')
                if pv_list:
                    for pv in pv_list:
                        value = pv.get('value')
                        synonyms_val = pv.get('synonyms')
                        if synonyms:
                            for synonym in synonyms_val:
                                if synonym:
                                    synonym = (synonym, value)
                                    synonym_set.add(synonym)

        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to pull synonyms from {synonym_url}, {get_exception_msg()}!"
            self.log.exception(msg)
            return None
        

