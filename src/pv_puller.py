#!/usr/bin/env python3

import json
import os
import schedule
import time
from bento.common.utils import get_logger
from common.constants import MODEL_FILE_DIR, TIER_CONFIG, CDE_COLLECTION, CDE_API_URL, CDE_CODE, CDE_VERSION, ID, CREATED_AT, UPDATED_AT
from common.utils import get_exception_msg, current_datetime, get_uuid_str, dump_dict_to_json, dump_dict_to_tsv, get_date_time
from common.pv_term_reader import TermReader
from common.api_client import APIInvoker

MODEL_DEFS = "models"
CADSR_DATA_ELEMENT = "DataElement"
CADSR_VALUE_DOMAIN = "ValueDomain"
CADSR_PERMISSIVE_VALUES = "PermissibleValues"

def pull_pv_lists(configs, mongo_dao):

    puller = PVPuller(configs, mongo_dao)
    # cron job to pull pv list from caDSR
    def job():
        print("PV puller task is running...")
        puller.pull_pv()
        print("PV puller is completed...")
    try:
        schedule.every().day.at("00:01").do(job)
        while True:
            schedule.run_pending()
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Task is stopped...")

"""
pull permissive values from CDE and save to db
"""
class PVPuller:
    
    def __init__(self, configs, mongo_dao):
        self.log = get_logger('Permissive values puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        # pull permissive values for testing
        self.pull_pv()

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
                    cde_id = term.get("Code")
                    if not cde_id:
                        self.log.error(f"No CDE id found for {data_common}:{prop_name}")
                        continue
                    cde_id = str(cde_id)
                    if not cde_id.isdigit(): # skip non-numeric cde codes
                        self.log.error(f"CDE id is invalid for {data_common}:{prop_name}: {cde_id}")
                        no_found_cde.append({"data_commons": data_common, "property": prop_name, "CDE_code": cde_id, "error": "Invalid CDE code."})
                        continue
                    cde_version = term.get("Version")
                    if not cde_version:
                        self.log.error(f"No CDE version found for {data_common}:{prop_name}: {cde_id}")
                        no_found_cde.append({"data_commons": data_common, "property": prop_name, "CDE_code": cde_id, "error": "Invalid CDE version."})
                        continue
                    # check if cde exists in db
                    # count = self.mongo_dao.count_docs(CDE_COLLECTION,{CDE_CODE: cde_id, CDE_VERSION: cde_version})
                    # if count > 0:
                    #     continue

                    # # 3) pull new cde permissive values and save to db
                    # api_client = APIInvoker(self.configs)
                    # result = api_client.get_data_element_by_cde_code(cde_id, self.configs[CDE_API_URL], cde_version)
                    # if not result or not result.get(CADSR_DATA_ELEMENT) or not result[CADSR_DATA_ELEMENT].get(CADSR_VALUE_DOMAIN):
                    #     self.log.error(f"No data element found for {data_common}/{prop_name}:{cde_id}:{cde_version}")
                    #     no_found_cde.append({"data_commons": data_common, "property": prop_name, "CDE_code": cde_id, "error": "No CDE element found."})
                    #     continue
                    # pv_list = result[CADSR_DATA_ELEMENT][CADSR_VALUE_DOMAIN].get(CADSR_PERMISSIVE_VALUES)
                    # if not pv_list or len(pv_list) == 0:
                    #     self.log.error(f"No permissive values found for {data_common}/{prop_name}:{cde_id}:{cde_version}")
                    #     no_found_cde.append({"data_commons": data_common, "property": prop_name, "CDE_code": cde_id, "error": "No CDE permissive values defined for the CDE code."})
                    #     pv_list = []
                    # else:
                    #     pv_list = [ item["value"] for item in pv_list]

                    # new_cde = {
                    #     ID: get_uuid_str(),
                    #     CDE_CODE: cde_id,
                    #     CDE_VERSION: cde_version,
                    #     CADSR_PERMISSIVE_VALUES: pv_list,
                    #     CREATED_AT: current_datetime(),
                    #     UPDATED_AT: current_datetime(),
                    # }
                    new_cde, msg = self.get_pv_by_code_version(data_common, prop_name, cde_id, cde_version)
                    if new_cde:
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
        
    def get_pv_by_code_version(self, data_common, prop_name, cde_code, cde_version):
        """
        get permissive values by cde code and version
        :param cde_code: cde code
        :param cde_version: cde version
        """
        # check if cde exists in db
        count = self.mongo_dao.count_docs(CDE_COLLECTION,{CDE_CODE: cde_code, CDE_VERSION: cde_version})
        if count > 0:
            return None, None

        # 3) pull new cde permissive values and save to db
        api_client = APIInvoker(self.configs)
        result = api_client.get_data_element_by_cde_code(cde_code, self.configs[CDE_API_URL], cde_version)
        if not result or not result.get(CADSR_DATA_ELEMENT) or not result[CADSR_DATA_ELEMENT].get(CADSR_VALUE_DOMAIN):
            self.log.error(f"No data element found for {data_common}/{prop_name}:{cde_code}:{cde_version}")
            return None, "No CDE element found."
        pv_list = result[CADSR_DATA_ELEMENT][CADSR_VALUE_DOMAIN].get(CADSR_PERMISSIVE_VALUES)
        if not pv_list or len(pv_list) == 0:
            self.log.error(f"No permissive values found for {data_common}/{prop_name}:{cde_code}:{cde_version}")
            return [],  "No CDE permissive values defined for the CDE code."
        else:
            pv_list = [ item["value"] for item in pv_list]

        return {
            ID: get_uuid_str(),
            CDE_CODE: cde_code,
            CDE_VERSION: cde_version,
            CADSR_PERMISSIVE_VALUES: pv_list,
            CREATED_AT: current_datetime(),
            UPDATED_AT: current_datetime(),
        }, None

