#!/usr/bin/env python3
import os
from bento.common.utils import get_logger
from common.constants import TIER_CONFIG, SYNONYM_API_URL
from common.utils import get_exception_msg
from common.api_client import APIInvoker

PERMISSIBLE_VALUES = "permissibleValues"
SYNONYMS = "synonyms"
VALUE = "value"

def pull_synonyms(configs, mongo_dao):
    log = get_logger('Synonym puller')
    puller = SynonymPuller(configs, mongo_dao)
    try:
        puller.pull_synonyms()
    except (KeyboardInterrupt, SystemExit):
        print("Task is stopped...")
    except Exception as e:
        log.critical(e)
        log.critical(
            f'Something wrong happened while pulling synonyms! Check debug log for details.')

"""
pull synonyms from sts and save to db
"""
class SynonymPuller:
    def __init__(self, configs, mongo_dao):
        self.log = get_logger('Permissive values puller')
        self.mongo_dao = mongo_dao
        self.configs = configs
        self.tier = self.configs[TIER_CONFIG]

    def pull_synonyms(self):
        # init a synonym set to make sure all synonym/pv pairs are unique
        synonym_set = set()
        try: 
        #  1) get contents in branch (tier) of the repo 
            synonym_url = str(self.configs[SYNONYM_API_URL])
            self.api_client = APIInvoker(self.configs)
            file_list = self.api_client.list_github_files(synonym_url, self.tier)
            if not file_list or len(file_list) == 0:
                self.log.info(f"Synonyms for {self.tier} are not found! ")
                return None
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
                if not (file["type"] == "file" and "_sts.json" in file["name"]):
                    continue
                # 2) pull synonyms from the file
                self.get_synonyms_by_datacommon_version(file["download_url"], synonym_set)

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
            synonyms  = [item for item in result if item.get(PERMISSIBLE_VALUES) and item.get(PERMISSIBLE_VALUES)[0].get(SYNONYMS)] 
            for item in synonyms:
                pv_list = item.get(PERMISSIBLE_VALUES)
                if pv_list:
                    for pv in pv_list:
                        value = pv.get(VALUE)
                        synonyms_val = pv.get(SYNONYMS)
                        if synonyms:
                            for synonym in synonyms_val:
                                if synonym:
                                    new_synonym = (synonym, value)
                                    synonym_set.add(new_synonym)

        except Exception as e:
            self.log.exception(e)
            msg = f"Failed to pull synonyms from {synonym_url}, {get_exception_msg()}!"
            self.log.exception(msg)
            return None
        