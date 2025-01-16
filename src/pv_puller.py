#!/usr/bin/env python3
from bento.common.utils import get_logger
from common.constants import MODEL_FILE_DIR, TIER_CONFIG, CDE_API_URL, CDE_CODE, CDE_VERSION, ID, CREATED_AT, UPDATED_AT,\
        TERM_CODE, TERM_VERSION
from common.utils import get_exception_msg, current_datetime, get_uuid_str, dump_dict_to_json, dump_dict_to_tsv, get_date_time
from common.pv_term_reader import TermReader
from common.api_client import APIInvoker

MODEL_DEFS = "models"
CADSR_DATA_ELEMENT = "DataElement"
CADSR_VALUE_DOMAIN = "ValueDomain"
CADSR_PERMISSIVE_VALUES = "PermissibleValues"
CADSR_DATA_ELEMENT_LONG_NAME = "longName"

def pull_pv_lists(configs, mongo_dao):
    log = get_logger('Permissive values puller')
    puller = PVPuller(configs, mongo_dao)
    try:
        puller.pull_pv()
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
    
    def __init__(self, configs, mongo_dao):
        self.log = get_logger('Permissive values puller')
        self.mongo_dao = mongo_dao
        self.configs = configs

    def pull_pv(self):
        # set env variables
        model_loc, tier= self.configs[MODEL_FILE_DIR], self.configs[TIER_CONFIG]
        new_cde_list = []
        new_cde = None
        no_found_cde = []
        try: 
           
        #  1) get terms
            pv_term_reader = TermReader(model_loc, tier)
            model_cde_terms = pv_term_reader.cdes

            if not model_cde_terms or  len(model_cde_terms.items()) == 0:
                raise Exception("No cde terms found!")
           
            dump_dict_to_json(model_cde_terms, f"models/data_terms.json")

        #  2)loop though all terms and check if the code and version existing db, pull new cde permissive values if not existing
            for data_common, props in model_cde_terms.items():
                for prop_name, term in props.items():
                    cde_id = term.get(TERM_CODE)
                    if not cde_id:
                        self.log.info(f"No CDE id found for {data_common}:{prop_name}")
                        continue
                    cde_origin = term.get("Origin")
                    if not cde_origin or not "caDSR" in cde_origin:
                        continue
                    cde_id = str(cde_id)
                    if not cde_id.isdigit(): # skip non-numeric cde codes
                        self.log.info(f"CDE id is invalid for {data_common}:{prop_name}: {cde_id}")
                        no_found_cde.append({"data_commons": data_common, "property": prop_name, "CDE_code": cde_id, "error": "Invalid CDE code."})
                        continue
                    cde_version = term.get(TERM_VERSION)
                    # check if cde exists in db
                    result = self.mongo_dao.get_cde_permissible_values(cde_id, cde_version)
                    if result:
                        continue
                    # check if cde exists in db, if not pull from CDE
                    new_cde, msg = get_pv_by_code_version(self.configs, self.log, data_common, prop_name, cde_id, cde_version)
                    if not new_cde is None:
                        # check if existing in new_cde_list
                        if not any(cde[CDE_CODE] == new_cde[CDE_CODE] and cde[CDE_VERSION] == new_cde[CDE_VERSION] for cde in new_cde_list):
                            new_cde_list.append(new_cde)
                    if msg:
                        no_found_cde.append({"data_commons": data_common, "property": prop_name, "CDE_code": cde_id, "error": msg})
                    
        #  4) save not found cde to tsv file
            if no_found_cde and len(no_found_cde) > 0:
                dump_dict_to_tsv(no_found_cde, f"tmp/not_found_cde_{get_date_time()}.tsv")

        #  5) save new cde permissive values to db
            if not new_cde_list or len(new_cde_list) == 0:
                self.log.info("No new CDE permissive values found!")
                return True
            self.mongo_dao.insert_cde(new_cde_list)
            return True

        except Exception as e: #catch all unhandled exception
            self.log.exception(e)
            msg = f"Failed to pull cde permissive values! {get_exception_msg()}!"
            self.log.exception(msg)
            return False
        
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
    pv_list = result[CADSR_DATA_ELEMENT][CADSR_VALUE_DOMAIN].get(CADSR_PERMISSIVE_VALUES)
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
        "CDEFullName": cde_long_name,
        CDE_CODE: cde_code,
        CDE_VERSION: cde_version,
        CADSR_PERMISSIVE_VALUES: pv_list,
        CREATED_AT: current_datetime(),
        UPDATED_AT: current_datetime(),
    }, msg

