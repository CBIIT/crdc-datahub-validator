import pytest
from unittest.mock import MagicMock, patch, call
from pv_puller_v2 import (
    PVPullerV2, 
    pull_pv_lists_v2,
    retrieveAllPropertyViaAPI,
    process_sts_property_pv,
    extract_pv_list,
    compose_property_record,
    compose_synonym_record,
    compose_concept_code_record,
    get_pv_by_property_version,
    get_all_pvs_by_version
)
from common.constants import (
    STS_API_ALL_URL_V2,
    STS_API_ONE_URL_V2,
    PROPERTY_PERMISSIBLE_VALUES,
    STS_DATA_RESOURCE_CONFIG,
    STS_DATA_RESOURCE_API,
    DATA_COMMONS_LIST,
    HIDDEN_MODELS,
    KEY,
    PROPERTY,
    MODEL,
    VERSION
)


# ==================== Fixtures ====================

@pytest.fixture
def mock_mongo_dao():
    """Mock MongoDB Data Access Object"""
    dao = MagicMock()
    dao.get_configuration_by_ev_var.return_value = [
        {KEY: ["model1", "model2", "model3"]},
        {KEY: ["model3"]}
    ]
    return dao


@pytest.fixture
def mock_api_client():
    """Mock API Client"""
    return MagicMock()


@pytest.fixture
def mock_configs():
    """Mock configuration dictionary"""
    return {
        STS_API_ALL_URL_V2: "https://sts-api.example.com/v1/property",
        STS_API_ONE_URL_V2: "https://sts-api.example.com/v1/property/{property}/version={version}",
        STS_DATA_RESOURCE_CONFIG: "api",
    }


@pytest.fixture
def mock_logger():
    """Mock logger"""
    logger = MagicMock()
    return logger


# ==================== Test PVPullerV2 Class ====================

@patch('pv_puller_v2.get_logger')
def test_pv_puller_v2_init_with_data_commons_and_hidden_models(mock_get_logger, mock_mongo_dao, mock_api_client, mock_configs):
    """Test PVPullerV2 initialization with data commons list and hidden models"""
    mock_get_logger.return_value = MagicMock()
    
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    
    assert puller.configs == mock_configs
    assert puller.mongo_dao == mock_mongo_dao
    assert puller.api_client == mock_api_client
    assert puller.pv_models == ["model1", "model2"]  # model3 is hidden


@patch('pv_puller_v2.get_logger')
def test_pv_puller_v2_init_no_configuration(mock_get_logger, mock_api_client, mock_configs):
    """Test PVPullerV2 initialization with no configuration"""
    mock_mongo_dao = MagicMock()
    mock_mongo_dao.get_configuration_by_ev_var.return_value = None
    mock_get_logger.return_value = MagicMock()
    
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    
    assert puller.pv_models == []


@patch('pv_puller_v2.get_logger')
def test_pv_puller_v2_init_only_one_config(mock_get_logger, mock_api_client, mock_configs):
    """Test PVPullerV2 initialization with only one configuration item"""
    mock_mongo_dao = MagicMock()
    mock_mongo_dao.get_configuration_by_ev_var.return_value = [{KEY: ["model1", "model2"]}]
    mock_get_logger.return_value = MagicMock()
    
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    
    assert puller.pv_models == []


@patch('pv_puller_v2.retrieveAllPropertyViaAPI')
@patch('pv_puller_v2.get_logger')
def test_pull_property_pv_synonym_concept_codes_success(
    mock_get_logger, 
    mock_retrieve_api,
    mock_mongo_dao, 
    mock_api_client, 
    mock_configs
):
    """Test successful retrieval and storage of properties, PVs, synonyms, and concept codes"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    # Mock successful API retrieval
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1", "val2"]},
        {PROPERTY: "prop2", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val3"]}
    ]
    synonym_records = {("synonym1", "val1"), ("synonym2", "val2")}
    concept_code_records = {("model1", "prop1", "val1", "code1")}
    
    mock_retrieve_api.return_value = (property_records, synonym_records, concept_code_records)
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    mock_mongo_dao.insert_concept_codes_v2.return_value = True
    
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    puller.pull_property_pv_synonym_concept_codes()
    
    # Verify API was called
    mock_retrieve_api.assert_called_once()
    
    # Verify MongoDB upsert was called
    mock_mongo_dao.upsert_property_pv.assert_called_once_with(property_records)
    
    # Verify synonyms were inserted
    mock_mongo_dao.insert_synonyms.assert_called_once_with(list(synonym_records))
    
    # Verify concept codes were inserted
    mock_mongo_dao.insert_concept_codes_v2.assert_called_once_with(list(concept_code_records))


@patch('pv_puller_v2.retrieveAllPropertyViaAPI')
@patch('pv_puller_v2.get_logger')
def test_pull_property_pv_synonym_concept_codes_no_properties(
    mock_get_logger,
    mock_retrieve_api,
    mock_mongo_dao,
    mock_api_client,
    mock_configs
):
    """Test behavior when no properties are retrieved"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    mock_retrieve_api.return_value = (None, None, None)
    
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    puller.pull_property_pv_synonym_concept_codes()
    
    # Verify no database operations when no properties
    mock_mongo_dao.upsert_property_pv.assert_not_called()


@patch('pv_puller_v2.retrieveAllPropertyViaAPI')
@patch('pv_puller_v2.get_logger')
def test_pull_property_pv_synonym_concept_codes_upsert_failure(
    mock_get_logger,
    mock_retrieve_api,
    mock_mongo_dao,
    mock_api_client,
    mock_configs
):
    """Test handling of upsert failure"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    property_records = [{PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0"}]
    
    mock_retrieve_api.return_value = (property_records, set(), set())
    mock_mongo_dao.upsert_property_pv.return_value = (False, "Database error")
    
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    puller.pull_property_pv_synonym_concept_codes()
    
    # Verify error logging
    mock_logger.error.assert_called()


# ==================== Test retrieveAllPropertyViaAPI ====================

@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_retrieve_all_property_via_api_success(
    mock_process_sts,
    mock_api_invoker_class,
    mock_configs,
    mock_logger
):
    """Test successful API retrieval of all properties"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    api_results = [{"property": "prop1"}, {"property": "prop2"}]
    processed_records = (
        [{"property": "prop1"}, {"property": "prop2"}],
        set(),
        set()
    )
    
    mock_api_client.get_all_data_elements_v2.return_value = [api_results]
    mock_process_sts.return_value = processed_records
    
    pv_models = ["model1", "model2"]
    result = retrieveAllPropertyViaAPI(mock_configs, pv_models, mock_logger)
    
    assert result == processed_records
    mock_api_client.get_all_data_elements_v2.assert_called_once()


@patch('pv_puller_v2.process_sts_property_pv')
def test_retrieve_all_property_via_api_with_explicit_client(
    mock_process_sts,
    mock_api_client,
    mock_configs,
    mock_logger
):
    """Test API retrieval with explicitly provided client"""
    api_results = [{"property": "prop1"}]
    processed_records = ([{"property": "prop1"}], set(), set())
    
    mock_api_client.get_all_data_elements_v2.return_value = [api_results]
    mock_process_sts.return_value = processed_records
    
    pv_models = ["model1"]
    result = retrieveAllPropertyViaAPI(mock_configs, pv_models, mock_logger, mock_api_client)
    
    assert result == processed_records


@patch('pv_puller_v2.process_sts_property_pv')
def test_retrieve_all_property_via_api_empty_models(
    mock_process_sts,
    mock_api_client,
    mock_configs,
    mock_logger
):
    """Test error when no models are configured"""
    with pytest.raises(Exception) as exc_info:
        retrieveAllPropertyViaAPI(mock_configs, [], mock_logger, mock_api_client)
    
    assert "No model configured" in str(exc_info.value)


@patch('pv_puller_v2.process_sts_property_pv')
def test_retrieve_all_property_via_api_no_results(
    mock_process_sts,
    mock_api_client,
    mock_configs,
    mock_logger
):
    """Test handling when API returns no results"""
    mock_api_client.get_all_data_elements_v2.return_value = [None, None]
    
    pv_models = ["model1", "model2"]
    
    # Should return None from process_sts_property_pv when no results
    mock_process_sts.return_value = (None, None, None)
    
    result = retrieveAllPropertyViaAPI(mock_configs, pv_models, mock_logger, mock_api_client)
    
    assert result == (None, None, None)


# ==================== Test process_sts_property_pv ====================

def test_process_sts_property_pv_success(mock_logger):
    """Test successful processing of STS API results"""
    sts_results = [
        {
            PROPERTY: "Property1",
            MODEL: "Model1",
            VERSION: "1.0.0",
            "permissibleValues": [
                {
                    "value": "value1",
                    "ncit_concept_code": "C12345",
                    "synonyms": ["syn1", "syn2"]
                }
            ]
        }
    ]
    
    property_records, synonym_set, concept_code_set = process_sts_property_pv(sts_results, mock_logger)
    
    assert len(property_records) == 1
    assert property_records[0][PROPERTY] == "Property1"
    assert len(synonym_set) == 2
    assert len(concept_code_set) == 1


def test_process_sts_property_pv_empty_results(mock_logger):
    """Test processing with empty results"""
    result = process_sts_property_pv([], mock_logger)
    
    assert result == (None, None, None)


def test_process_sts_property_pv_null_property(mock_logger):
    """Test processing with null property values"""
    sts_results = [
        {
            PROPERTY: "null",
            MODEL: "Model1",
            VERSION: "1.0.0"
        }
    ]
    
    result = process_sts_property_pv(sts_results, mock_logger)
    
    assert result == (None, None, None)


def test_process_sts_property_pv_duplicate_properties(mock_logger):
    """Test that duplicate properties are not added to records"""
    sts_results = [
        {
            PROPERTY: "Property1",
            MODEL: "Model1",
            VERSION: "1.0.0",
            "permissibleValues": []
        },
        {
            PROPERTY: "Property1",
            MODEL: "Model1",
            VERSION: "1.0.0",
            "permissibleValues": []
        }
    ]
    
    property_records, _, _ = process_sts_property_pv(sts_results, mock_logger)
    
    assert len(property_records) == 1


def test_process_sts_property_pv_property_only_flag(mock_logger):
    """Test processing with property_only flag"""
    sts_results = [
        {
            PROPERTY: "Property1",
            MODEL: "Model1",
            VERSION: "1.0.0",
            "permissibleValues": [
                {
                    "value": "value1",
                    "synonyms": ["syn1"]
                }
            ]
        }
    ]
    
    property_records, synonym_set, concept_code_set = process_sts_property_pv(
        sts_results, mock_logger, property_only=True
    )
    
    assert len(property_records) == 1
    assert len(synonym_set) == 0
    assert len(concept_code_set) == 0


# ==================== Test extract_pv_list ====================

def test_extract_pv_list_valid_values():
    """Test extraction of valid permissible values"""
    pv_list = [
        {"value": "value1"},
        {"value": "value2"},
        {"value": "value3"}
    ]
    
    result = extract_pv_list(pv_list)
    
    assert result == ["value1", "value2", "value3"]


def test_extract_pv_list_with_whitespace():
    """Test extraction with whitespace stripping"""
    pv_list = [
        {"value": "  value1  "},
        {"value": "value2\n"}
    ]
    
    result = extract_pv_list(pv_list)
    
    assert result == ["value1", "value2"]


def test_extract_pv_list_empty():
    """Test extraction from empty list"""
    result = extract_pv_list([])
    
    assert result == []


def test_extract_pv_list_none_values():
    """Test extraction with None values"""
    pv_list = [
        {"value": "value1"},
        {"value": None},
        {"value": "value2"}
    ]
    
    result = extract_pv_list(pv_list)
    
    assert result == ["value1", "value2"]


# ==================== Test compose_property_record ====================

def test_compose_property_record_complete():
    """Test composing a complete property record"""
    property_item = {
        PROPERTY: "Property1",
        MODEL: "Model1",
        VERSION: "1.0.0",
        "permissibleValues": [
            {"value": "val1"},
            {"value": "val2"}
        ]
    }
    
    record = compose_property_record(property_item)
    
    assert record[PROPERTY] == "Property1"
    assert record[MODEL] == "Model1"
    assert record[VERSION] == "1.0.0"
    assert len(record[PROPERTY_PERMISSIBLE_VALUES]) == 2


def test_compose_property_record_with_complex_version():
    """Test version extraction from complex version string"""
    property_item = {
        PROPERTY: "Property1",
        MODEL: "Model1",
        VERSION: "1.2.3.4.alpha.rc1",
        "permissibleValues": []
    }
    
    record = compose_property_record(property_item)
    
    # The regex [\d.]+ matches all consecutive digits and dots
    assert record[VERSION] == "1.2.3.4."


def test_compose_property_record_null_version():
    """Test handling of null version"""
    property_item = {
        PROPERTY: "Property1",
        MODEL: "Model1",
        VERSION: "null",
        "permissibleValues": []
    }
    
    record = compose_property_record(property_item)
    
    assert record[VERSION] is None


# ==================== Test compose_synonym_record ====================

def test_compose_synonym_record_success():
    """Test successful synonym record composition"""
    property_item = {
        "permissibleValues": [
            {
                "value": "val1",
                "synonyms": ["syn1", "syn2"]
            },
            {
                "value": "val2",
                "synonyms": ["syn3"]
            }
        ]
    }
    
    synonym_set = set()
    compose_synonym_record(property_item, synonym_set)
    
    assert len(synonym_set) == 3
    assert ("syn1", "val1") in synonym_set
    assert ("syn2", "val1") in synonym_set
    assert ("syn3", "val2") in synonym_set


def test_compose_synonym_record_no_synonyms():
    """Test when there are no synonyms"""
    property_item = {
        "permissibleValues": [
            {"value": "val1"}
        ]
    }
    
    synonym_set = set()
    compose_synonym_record(property_item, synonym_set)
    
    assert len(synonym_set) == 0


def test_compose_synonym_record_duplicate_synonyms():
    """Test that duplicate synonyms are not added"""
    property_item = {
        "permissibleValues": [
            {
                "value": "val1",
                "synonyms": ["syn1", "syn1"]
            }
        ]
    }
    
    synonym_set = set()
    compose_synonym_record(property_item, synonym_set)
    
    # Sets automatically handle duplicates
    assert len(synonym_set) == 1


def test_compose_synonym_record_lowercases_term():
    """Synonym terms are stored lowercase; case variants dedupe."""
    property_item = {
        "permissibleValues": [
            {
                "value": "val1",
                "synonyms": ["Foo", " FOO ", "foo"]
            }
        ]
    }
    synonym_set = set()
    compose_synonym_record(property_item, synonym_set)
    assert len(synonym_set) == 1
    assert ("foo", "val1") in synonym_set


def test_compose_synonym_record_none_pv_list():
    """Test with None permissibleValues list"""
    property_item = {"permissibleValues": None}
    
    synonym_set = set()
    compose_synonym_record(property_item, synonym_set)
    
    assert len(synonym_set) == 0


# ==================== Test compose_concept_code_record ====================

def test_compose_concept_code_record_success():
    """Test successful concept code record composition"""
    property_item = {
        MODEL: "Model1",
        PROPERTY: "Property1",
        "permissibleValues": [
            {
                "value": "val1",
                "ncit_concept_code": "C12345"
            },
            {
                "value": "val2",
                "ncit_concept_code": "C67890"
            }
        ]
    }
    
    concept_code_set = set()
    compose_concept_code_record(property_item, concept_code_set)
    
    assert len(concept_code_set) == 2
    assert ("Model1", "Property1", "val1", "C12345") in concept_code_set
    assert ("Model1", "Property1", "val2", "C67890") in concept_code_set


def test_compose_concept_code_record_no_concept_codes():
    """Test when there are no concept codes"""
    property_item = {
        MODEL: "Model1",
        PROPERTY: "Property1",
        "permissibleValues": [
            {"value": "val1"}
        ]
    }
    
    concept_code_set = set()
    compose_concept_code_record(property_item, concept_code_set)
    
    assert len(concept_code_set) == 0


def test_compose_concept_code_record_none_pv_list():
    """Test with None permissibleValues list"""
    property_item = {
        MODEL: "Model1",
        PROPERTY: "Property1",
        "permissibleValues": None
    }
    
    concept_code_set = set()
    compose_concept_code_record(property_item, concept_code_set)
    
    assert len(concept_code_set) == 0


# ==================== Test get_pv_by_property_version ====================

@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_pv_by_property_version_success(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test successful retrieval of PVs by property and version"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, set(), set())
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    
    result = get_pv_by_property_version(mock_configs, mock_logger, "prop1", "1.0", "model1", mock_mongo_dao)
    
    assert result == property_records[0]


@patch('pv_puller_v2.APIInvoker')
def test_get_pv_by_property_version_no_api_url(
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_logger
):
    """Test error when STS API URL is not configured"""
    # Create a config with STS_API_ONE_URL_V2 key but empty value
    configs = {STS_API_ONE_URL_V2: ""}
    
    result = get_pv_by_property_version(configs, mock_logger, "prop1", "1.0", "model1", mock_mongo_dao)
    
    assert result is None


@patch('pv_puller_v2.get_all_pvs_by_version')
@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_pv_by_property_version_no_version(
    mock_process_sts,
    mock_api_invoker_class,
    mock_get_all_pvs,
    mock_mongo_dao,
    mock_logger
):
    """When version is None, no record matches (API returns records with concrete version e.g. "1.0"), so function returns None."""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    # Use URL format that matches the replace pattern in the code: /version={prop_version}
    configs = {
        STS_API_ONE_URL_V2: "https://sts-api.example.com/v1/property/{property}/version={prop_version}",
        STS_API_ALL_URL_V2: "https://sts-api.example.com/v1/property"
    }
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, set(), set())
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    
    result = get_pv_by_property_version(configs, mock_logger, "prop1", None, "model1", mock_mongo_dao)
    
    # When prop_version is None, next() at line 274 finds no match (item[VERSION]="1.0" != None),
    # so the function correctly returns None.
    assert result is None


# ==================== Test pull_pv_lists_v2 ====================

@patch('pv_puller_v2.PVPullerV2')
@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.get_logger')
def test_pull_pv_lists_v2_success(
    mock_get_logger,
    mock_api_invoker_class,
    mock_puller_class,
    mock_mongo_dao,
    mock_configs
):
    """Test successful pull_pv_lists_v2 execution"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    mock_puller = MagicMock()
    mock_puller_class.return_value = mock_puller
    
    pull_pv_lists_v2(mock_configs, mock_mongo_dao)
    
    # Verify PVPullerV2 was instantiated
    mock_puller_class.assert_called_once_with(mock_configs, mock_mongo_dao, mock_api_invoker_class.return_value)
    
    # Verify pull method was called
    mock_puller.pull_property_pv_synonym_concept_codes.assert_called_once()


@patch('pv_puller_v2.PVPullerV2')
@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.get_logger')
def test_pull_pv_lists_v2_keyboard_interrupt(
    mock_get_logger,
    mock_api_invoker_class,
    mock_puller_class,
    mock_mongo_dao,
    mock_configs
):
    """Test handling of KeyboardInterrupt"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    mock_puller = MagicMock()
    mock_puller_class.return_value = mock_puller
    mock_puller.pull_property_pv_synonym_concept_codes.side_effect = KeyboardInterrupt()
    
    # Should not raise exception
    pull_pv_lists_v2(mock_configs, mock_mongo_dao)


@patch('pv_puller_v2.PVPullerV2')
@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.get_logger')
def test_pull_pv_lists_v2_exception(
    mock_get_logger,
    mock_api_invoker_class,
    mock_puller_class,
    mock_mongo_dao,
    mock_configs
):
    """Test handling of general exceptions"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    mock_puller = MagicMock()
    mock_puller_class.return_value = mock_puller
    mock_puller.pull_property_pv_synonym_concept_codes.side_effect = Exception("Test error")
    
    # Should catch exception and log it
    pull_pv_lists_v2(mock_configs, mock_mongo_dao)
    
    # Verify error was logged
    mock_logger.critical.assert_called()


# ==================== Test get_all_pvs_by_version ====================

@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_success(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test successful retrieval and save of all PVs, synonyms, and concept codes"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1", "val2"]}
    ]
    synonym_records = {("syn1", "val1"), ("syn2", "val2")}
    concept_code_records = {("model1", "prop1", "val1", "C123"), ("model1", "prop1", "val2", "C456")}
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, synonym_records, concept_code_records)
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    mock_mongo_dao.insert_concept_codes_v2.return_value = True
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify API was called with correct URL
    expected_url = f"{mock_configs[STS_API_ALL_URL_V2]}/model1/?version=1.0"
    mock_api_client.get_all_data_elements.assert_called_once_with(expected_url)
    
    # Verify data was processed
    mock_process_sts.assert_called_once()
    
    # Verify data was saved
    mock_mongo_dao.upsert_property_pv.assert_called_once_with(property_records)
    mock_mongo_dao.insert_synonyms.assert_called_once_with(list(synonym_records))
    mock_mongo_dao.insert_concept_codes_v2.assert_called_once_with(list(concept_code_records))


@patch('pv_puller_v2.APIInvoker')
def test_get_all_pvs_by_version_no_api_url(
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_logger
):
    """Test behavior when STS API URL is not configured"""
    configs = {STS_API_ONE_URL_V2: ""}  # Empty string means not configured
    
    result = get_all_pvs_by_version(configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    assert result is None
    mock_logger.error.assert_called_with("Invalid STS API URL.")


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_no_properties(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test behavior when no properties are retrieved"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    mock_api_client.get_all_data_elements.return_value = []
    mock_process_sts.return_value = (None, set(), set())
    
    mock_mongo_dao.upsert_property_pv.return_value = (False, "Failed")
    
    result = get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Should return None when no properties
    assert result is None


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_no_synonyms(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test behavior when no synonyms are found"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, set(), set())  # Empty synonyms set
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify "No synonym found!" was logged
    assert any("No synonym found!" in str(call) for call in mock_logger.info.call_args_list)


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_no_concept_codes(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test behavior when no concept codes are found"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    synonym_records = {("syn1", "val1")}
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, synonym_records, set())  # Empty concept codes set
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    mock_mongo_dao.insert_concept_codes_v2.return_value = True
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify "No concept code found!" was logged
    assert any("No concept code found!" in str(call) for call in mock_logger.info.call_args_list)


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_upsert_failure(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test handling when upsert of properties fails"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, set(), set())
    
    mock_mongo_dao.upsert_property_pv.return_value = (False, "Database error")
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify error was logged
    mock_logger.error.assert_called_with("Failed to pull and save Property PV!")


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_insert_synonyms_none(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test handling when insert_synonyms returns None"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    synonym_records = {("syn1", "val1")}
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, synonym_records, set())
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = None  # Returns None indicating failure
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify insert_synonyms was called
    mock_mongo_dao.insert_synonyms.assert_called_once()


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_insert_concept_codes_none(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test handling when insert_concept_codes_v2 returns None"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]}
    ]
    synonym_records = {("syn1", "val1")}
    concept_code_records = {("model1", "prop1", "val1", "C123")}
    
    mock_api_client.get_all_data_elements.return_value = [{"property": "prop1"}]
    mock_process_sts.return_value = (property_records, synonym_records, concept_code_records)
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    mock_mongo_dao.insert_concept_codes_v2.return_value = None  # Returns None indicating failure
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify insert_concept_codes_v2 was called
    mock_mongo_dao.insert_concept_codes_v2.assert_called_once()


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_api_exception(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test handling of API exceptions"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    # API throws exception
    mock_api_client.get_all_data_elements.side_effect = Exception("API connection error")
    
    # Should raise exception (not caught by this function)
    with pytest.raises(Exception) as exc_info:
        get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    assert "API connection error" in str(exc_info.value)


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_multiple_records(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test with multiple properties, synonyms, and concept codes"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    property_records = [
        {PROPERTY: "prop1", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val1"]},
        {PROPERTY: "prop2", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val2"]},
        {PROPERTY: "prop3", MODEL: "model1", VERSION: "1.0", PROPERTY_PERMISSIBLE_VALUES: ["val3"]}
    ]
    synonym_records = {("syn1", "val1"), ("syn2", "val1"), ("syn3", "val2"), ("syn4", "val3")}
    concept_code_records = {
        ("model1", "prop1", "val1", "C111"),
        ("model1", "prop2", "val2", "C222"),
        ("model1", "prop3", "val3", "C333")
    }
    
    mock_api_client.get_all_data_elements.return_value = [
        {"property": "prop1"},
        {"property": "prop2"},
        {"property": "prop3"}
    ]
    mock_process_sts.return_value = (property_records, synonym_records, concept_code_records)
    
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    mock_mongo_dao.insert_concept_codes_v2.return_value = True
    
    get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Verify all data was saved
    mock_mongo_dao.upsert_property_pv.assert_called_once_with(property_records)
    assert len(mock_mongo_dao.insert_synonyms.call_args[0][0]) == 4
    assert len(mock_mongo_dao.insert_concept_codes_v2.call_args[0][0]) == 3


@patch('pv_puller_v2.APIInvoker')
@patch('pv_puller_v2.process_sts_property_pv')
def test_get_all_pvs_by_version_empty_results_from_api(
    mock_process_sts,
    mock_api_invoker_class,
    mock_mongo_dao,
    mock_configs,
    mock_logger
):
    """Test handling when API returns empty results"""
    mock_api_client = MagicMock()
    mock_api_invoker_class.return_value = mock_api_client
    
    mock_api_client.get_all_data_elements.return_value = None
    mock_process_sts.return_value = (None, None, None)
    
    mock_mongo_dao.upsert_property_pv.return_value = (False, "No data")
    
    result = get_all_pvs_by_version(mock_configs, mock_logger, "1.0", "model1", mock_mongo_dao)
    
    # Should return None when no properties
    assert result is None


# ==================== Integration Tests ====================

@patch('pv_puller_v2.get_logger')
def test_pv_puller_integration_full_flow(
    mock_get_logger,
    mock_mongo_dao,
    mock_api_client,
    mock_configs
):
    """Integration test for full PVPuller flow"""
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    
    # Setup API client mock to return realistic data
    api_results = [{
        PROPERTY: "test_property",
        MODEL: "test_model",
        VERSION: "1.0.0",
        "permissibleValues": [
            {
                "value": "test_value",
                "synonyms": ["test_synonym"],
                "ncit_concept_code": "C123"
            }
        ]
    }]
    
    mock_api_client.get_all_data_elements_v2.return_value = [api_results]
    
    # Setup MongoDB mock
    mock_mongo_dao.upsert_property_pv.return_value = (True, "Success")
    mock_mongo_dao.insert_synonyms.return_value = True
    mock_mongo_dao.insert_concept_codes_v2.return_value = True
    
    # Create puller and run
    puller = PVPullerV2(mock_configs, mock_mongo_dao, mock_api_client)
    puller.pull_property_pv_synonym_concept_codes()
    
    # Verify expected calls
    assert mock_mongo_dao.upsert_property_pv.called
    assert mock_mongo_dao.insert_synonyms.called
    assert mock_mongo_dao.insert_concept_codes_v2.called
