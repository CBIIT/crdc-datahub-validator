// insert one configuration for STS resource
db.configuration.insertOne({
    "_id": "8b66cdf3-a10a-4977-acdb-fd14f4eef2ea",
    "type": "STS_RESOURCE",
    "keys": {
        "sts_data_resource": "sts_api",
        "sts-dump-file-url": "https://raw.githubusercontent.com/CBIIT/crdc-datahub-terms/{}/mdb_pvs_synonyms.json",
        "sts_api_all_url": "https://sts-dev.cancer.gov/all-pvs?format=json",
        "sts_api_one_url": "https://sts-dev.cancer.gov/cde-pvs/{cde_code}/{cde_version}?format=json",
        "sts_api_all_url_v2": "https://sts-dev.cancer.gov/v2/terms/model-pvs",
        "sts_api_one_url_v2": "https://sts-dev.cancer.gov/v2/terms/model-pvs/{model}/{property}?version={version}"


    }
}); 