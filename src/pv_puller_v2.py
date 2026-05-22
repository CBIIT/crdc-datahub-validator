#!/usr/bin/env python3
from bento.common.utils import get_logger
from common.constants import STS_API_ALL_URL_V2, STS_API_ONE_URL_V2, \
        PROPERTY_PERMISSIBLE_VALUES, STS_DATA_RESOURCE_CONFIG, STS_DATA_RESOURCE_API, DATA_COMMONS_LIST, HIDDEN_MODELS, KEY, PROPERTY, MODEL, VERSION
from common.utils import get_exception_msg
from common.api_client import APIInvoker
import re

MODEL_DEFS = "models"
CADSR_DATA_ELEMENT = "DataElement"
CADSR_VALUE_DOMAIN = "ValueDomain"
CADSR_DATA_ELEMENT_LONG_NAME = "longName"
CADSR_PERMISSIVE_VALUES = "PermissibleValues"
FILE_DOWNLOAD_URL = "download_url"
FILE_NAME = "name"
FILE_TYPE = "type"
PROPERTY_PV_NAME = "permissibleValues"
NCIT_PROPERTY_CONCEPT_CODE = "ncit_concept_code"
NCIT_SYNONYMS = "synonyms"
NCIT_VALUE = "value"

def pull_pv_lists_v2(configs, mongo_dao):
    """
    Pull permissible values and synonyms from STS and save them to the database.
    
    :param configs: Configuration settings for the puller.
    :param mongo_dao: Data access object for MongoDB operations.
    """
    log = get_logger('Permissive values and synonym puller')
    api_client = APIInvoker(configs)
    pv_puller = PVPullerV2(configs, mongo_dao, api_client)
    # synonym_puller = SynonymPuller(configs, mongo_dao, api_client)
    
    try:
        # pull pv, property, synonym, concept codes
        pv_puller.pull_property_pv_synonym_concept_codes()
    except (KeyboardInterrupt, SystemExit):
        print("Task is stopped...")
    except Exception as e:
        log.critical(e)
        log.critical(
            f'Something wrong happened while pulling permissive values! Check debug log for details.')
class PVPullerV2:
    """
    Class for pulling permissible values from STS and saving them to the database.
    """
    def __init__(self, configs, mongo_dao, api_client):
        self.log = get_logger('Permissive values puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        self.api_client = api_client
        self.config_model_list = self.mongo_dao.get_configuration_by_ev_var([DATA_COMMONS_LIST, HIDDEN_MODELS])
        if self.config_model_list is not None:
            if len(self.config_model_list) == 2: #if both data commons list and hidden models are configured
                self.pv_models = [x for x in self.config_model_list[0][KEY] if x not in self.config_model_list[1][KEY]]
            else:
                self.pv_models = []
        else:
            self.pv_models = []
        
    def pull_property_pv_synonym_concept_codes(self):
        """
        pull property pv from STS API (STS_API_ALL_URL_V2) and save to db
        """
        resource = self.configs[STS_DATA_RESOURCE_CONFIG] if self.configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_API
        # resource = self.configs[STS_DATA_RESOURCE_CONFIG] if self.configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_FILE
        try:
            property_records, synonym_records, concept_codes_records = retrieveAllPropertyViaAPI(self.configs, self.pv_models, self.log, self.api_client)
            if not property_records or len(property_records) == 0:
                self.log.info("No property found!")
                return
            self.log.info(f"{len(property_records)} unique property are retrieved!")
            result, msg = self.mongo_dao.upsert_property_pv(list(property_records))
            if result: 
                self.log.info(f"Property PV are pulled and save successfully!")
            else:
                self.log.error(f"Failed to pull and save Property PV! {msg}")

            if not synonym_records or len(synonym_records) == 0:
                self.log.info("No synonym found!")
                return
            self.log.info(f"{len(synonym_records)} unique synonyms are retrieved!")
            result = self.mongo_dao.insert_synonyms(list(synonym_records))
            if result is not None:
                self.log.info(f"Property Synonyms are pulled and save successfully!")

            if not concept_codes_records or len(concept_codes_records) == 0:
                self.log.info("No concept code found!")
                return
            self.log.info(f"{len(concept_codes_records)} unique concept codes are retrieved!")
            result = self.mongo_dao.insert_concept_codes_v2(list(concept_codes_records))
            if result is not None:
                self.log.info(f"Property Concept Codes are pulled and save successfully!")
            self.log.info(f"All property PVs, Synonyms and Concept Codes are pulled and saved successfully!")
            return
        except Exception as e:
            self.log.exception(e)
            self.log.exception(f"Failed to retrieve property PVs.")

def retrieveAllPropertyViaAPI(configs, pv_models, log, api_client=None):
    """
    extract property from api
    """
    sts_api_url_list = []
    if len(pv_models) > 0:
        for pv_model in pv_models:
            sts_api_url_list.append(f"{configs[STS_API_ALL_URL_V2]}/{pv_model}")
    else:
        raise Exception("No model configured for pulling property PVs.")
    log.info(f"Retrieving property from {sts_api_url_list}...")
    if not api_client:
        api_client = APIInvoker(configs)
    results_list = api_client.get_all_data_elements_v2(sts_api_url_list)
    results = []
    for result in results_list:
        if result is not None:
            results.extend(result)
    if not results or len(results) == 0:
        log.error(f"No property/pvs retrieve from STS API, {sts_api_url_list}.")
        return None, None, None
    property_records, synonym_set, concept_code_set = process_sts_property_pv(results, log)
    log.info(f"Retrieved property PVs from {sts_api_url_list}.")
    return property_records, synonym_set, concept_code_set


def process_sts_property_pv(sts_results, log, property_only=False):
    """
    get property pv from sts api
    :param sts_api_url: sts api url
    """
    property_set = set()
    property_records = []
    synonym_set = set()
    concept_code_set = set()
    if not sts_results or len(sts_results) == 0:
        log.error(f"No property/pvs retrieve from STS API.")
        return None, None, None
    property_list  = [item for item in sts_results if item.get(PROPERTY) and item.get(PROPERTY) != 'null'] 
    if not property_list or len(property_list) == 0:
        log.error(f"No property found in STS API results.")
        return None, None, None
    for item in property_list:
        property_name = item.get(PROPERTY)
        model = item.get(MODEL) if item.get(MODEL) and item.get(MODEL) != 'null' else None
        version = item.get(VERSION) if item.get(VERSION) and item.get(VERSION) != 'null' else None
        version = re.match(r'[\d.]+', version).group()
        property_key = (property_name, model, version)
        if property_key in property_set:
            continue
        property_set.add(property_key)
        property_record = compose_property_record(item)
        property_records.append(property_record)
        if property_only:
            continue
        # extract synonyms
        if item.get(PROPERTY_PV_NAME) and len(item.get(PROPERTY_PV_NAME)) > 0 and item.get(PROPERTY_PV_NAME)[0].get(NCIT_SYNONYMS):
            compose_synonym_record(item, synonym_set)

        # extract concept codes
        if item.get(PROPERTY_PV_NAME) and len(item.get(PROPERTY_PV_NAME)) > 0 and item.get(PROPERTY_PV_NAME)[0].get(NCIT_PROPERTY_CONCEPT_CODE):
            compose_concept_code_record(item, concept_code_set)

    return property_records, synonym_set, concept_code_set

def extract_pv_list(property_pv_list):
    """
    extract pv list from property pv list
    """
    pv_list = []
    if property_pv_list is None:
        pv_list = None
    if property_pv_list and any(item.get(NCIT_VALUE) for item in property_pv_list):
        pv_list = [item[NCIT_VALUE] for item in property_pv_list if NCIT_VALUE in item and item[NCIT_VALUE] is not None]
        # strip white space if the value is a string
        if pv_list and isinstance(pv_list[0], str): 
            pv_list = [item.strip() for item in pv_list]
    
    return pv_list

def compose_property_record(property_item):
    """
    compose property record from property item
    """
    property_record = {
        PROPERTY: property_item.get(PROPERTY),
        MODEL: property_item.get(MODEL),
        VERSION: re.match(r'[\d.]+', property_item.get(VERSION)).group() if property_item.get(VERSION) and property_item.get(VERSION) != 'null' else None,
        PROPERTY_PERMISSIBLE_VALUES: extract_pv_list(property_item.get(PROPERTY_PV_NAME))
    }
    return property_record

def compose_synonym_record(property_item, synonym_set):
    """
    compose synonym record from property item
    Synonym terms are normalized to lowercase before deduplication and DB insert.
    """
    pv_list = property_item.get(PROPERTY_PV_NAME)
    if pv_list:
        for pv_item in pv_list:
            synonyms = pv_item.get(NCIT_SYNONYMS)
            if synonyms:
                for synonym in synonyms:
                        if synonym:
                            term = str(synonym).strip().lower()
                            if not term:
                                continue
                            synonym_key = (term, pv_item.get(NCIT_VALUE))
                            if synonym_key in synonym_set:
                                continue
                            synonym_set.add(synonym_key)
    return

def compose_concept_code_record(property_item, concept_code_set):
    """
    compose concept code record from property item
    """
    pv_list = property_item.get(PROPERTY_PV_NAME)
    model = property_item.get(MODEL)
    property_name = property_item.get(PROPERTY)
    if pv_list:
        for pv in pv_list:
            value = pv.get(NCIT_VALUE)
            concept_code = pv.get(NCIT_PROPERTY_CONCEPT_CODE)
            if concept_code:
                concept_code_key = (model, property_name, value, concept_code)
                if concept_code_key in concept_code_set:
                    continue
                concept_code_set.add(concept_code_key)
    return

def get_pv_by_property_version(configs, log, prop, prop_version, prop_model, mongo_dao):
    """
    get all permissive values by property,version, and model
    """
    msg = None
    prop_records = []
    api_client = APIInvoker(configs)
    #resource = configs[STS_DATA_RESOURCE_CONFIG] if configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_API
    # resource = configs[STS_DATA_RESOURCE_CONFIG] if configs.get(STS_DATA_RESOURCE_CONFIG) else STS_DATA_RESOURCE_FILE
    sts_api_url = configs[STS_API_ONE_URL_V2]
    if not sts_api_url:
        msg = "STS API url is not configured."
        log.error(f"Invalid STS API URL.")
        return None
    if not prop_version:
        sts_api_url = sts_api_url.replace("/version={prop_version}", "")
        sts_api_url = sts_api_url.format(property=prop)
        prop_version = None
    else:   
        sts_api_url = sts_api_url.format(model=prop_model, property=prop, version=prop_version)
    log.info(f"Retrieving property values from {sts_api_url} for {prop}/{prop_version}...")
    try:
        results = api_client.get_all_data_elements(sts_api_url)
        prop_records, _, _ = process_sts_property_pv(results, log, True)
        if not prop_records or len(prop_records) == 0:
            msg = f"No property found for {prop}/{prop_version}."
            log.info(msg)
            return None
    except Exception as e:
        log.exception(e)
        msg = f"Failed to retrieve property PVs for {prop}/{prop_version}."
        log.exception(msg)
        return None
    except Exception as e:
        log.exception(e)
        msg = f"Failed to retrieve property PVs for {prop}/{prop_version}."
        log.exception(msg)
        return None

    if not prop_records or len(prop_records) == 0:
        msg = f"No property found for {prop}/{prop_version}."
        log.info(msg)
        return None
    log.info(f"{len(prop_records)} unique properties are retrieved!")
    prop_record = next((item for item in prop_records if item[PROPERTY] == prop and item[MODEL] == prop_model and item[VERSION] == prop_version), None)
    if not prop_record:
        msg = f"No property found for {prop}/{prop_version}."
        log.info(msg)
        return None
    if not prop_records or len(prop_records) == 0:
        msg = f"No property found for {prop}/{prop_version}."
        log.info(msg)
        return None
    log.info(f"{len(prop_records)} unique properties are retrieved!")
    log.info(f"Retrieved property for {prop}/{prop_version}.")
    # save property pv to db
    result, _ = mongo_dao.upsert_property_pv([prop_record])
    get_all_pvs_by_version(configs, log, prop_version, prop_model, mongo_dao)
    if result:
        log.info(f"Property PV are pulled and save successfully!")
    else:
        log.error(f"Failed to pull and save property PV! {msg}")
    return prop_record

def get_all_pvs_by_version(configs, log, version, model, mongo_dao):
    api_client = APIInvoker(configs)
    sts_api_url = configs[STS_API_ONE_URL_V2]
    if not sts_api_url:
        log.error(f"Invalid STS API URL.")
        return None
    sts_api_url = f"{configs[STS_API_ALL_URL_V2]}/{model}/?version={version}"
    results = api_client.get_all_data_elements(sts_api_url)
    prop_records, synonym_records, concept_codes_records = process_sts_property_pv(results, log, False)
    result_pv, _ = mongo_dao.upsert_property_pv(prop_records)
    if result_pv: 
        log.info(f"Property PV for {model}/{version} are pulled and save successfully!")
    else:
        log.error(f"Failed to pull and save Property PV!")
    if not prop_records or len(prop_records) == 0:
        log.info("No property found!")
        return None

    if not synonym_records or len(synonym_records) == 0:
        log.info("No synonym found!")
    log.info(f"{len(synonym_records)} unique synonyms are retrieved!")
    result_synonyms = mongo_dao.insert_synonyms(list(synonym_records))
    if result_synonyms is not None:
        log.info(f"Property Synonyms for {model}/{version} are pulled and save successfully!")

    if not concept_codes_records or len(concept_codes_records) == 0:
        log.info("No concept code found!")
    log.info(f"{len(concept_codes_records)} unique concept codes are retrieved!")
    result_concept_codes = mongo_dao.insert_concept_codes_v2(list(concept_codes_records))
    if result_concept_codes is not None:
        log.info(f"Property Concept Codes for {model}/{version} are pulled and save successfully!")
    log.info(f"All property PVs, Synonyms and Concept Codes for {model}/{version} are pulled and saved successfully!")
    
    

