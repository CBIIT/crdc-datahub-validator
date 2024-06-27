# crdc-datahub-validator

CRDC datahub Validator is a linux service application for validating metadata and file.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3 and so on. All required python modules is listed in the file, requirements.txt, and .gitmodules.

The application is consist of multiple python modules/classes to support multiple functions listed below:

1) Linux services that keeps poll crdc databub message in specific AWS SQS queues for different service types, essential, file and metadata validation services respectively.
2) Metadata model factory that stores models in python dict for different data commons after the service started.  It also provides different mode access functions.
3) Metadata model reader that is called by the metadata model factory to parse data model yaml files currently and is expendable for different model sources.
4) Mongo database access layer for services to retrieve batch detail, loading metadata into DB and update batch or dataRecords after validation and data loading.
5) File downloader that get metadata file objects from S3 bucket based on batch.
6) Essential validator, data file validator and metadata validator to validate tsv file and/or contents.
7) Data loader thats insert, update or delete validated data in tsv file to Mongo database.
8) Log info, error and exceptions.

Major implemented modules/classes in src dir:

1) validator.py
    This is the entry point of the command line interface.  It controls the workflow and dispatches to different service based on configured service type.

2) config.py
    This class manages request arguments and configurations.  It receives user's arguments, validates these arguments and stores them in a dictionary.

3) essential_validator.py contains:
    A service that poll messages in a specific sqs queue, loader_queue, and call essential validator class for validation and metadata loading.
    A class, EssentialValidator,  validates 1) if batch object contains all required fields; 2) check if files in the batch are metadata; 3) check if tsv file contents are valid. 4) if the batch matadata intention is new, verify no existing records are matched with ids of the data in the files.

4) data_loader.py
    This class loads validated metadata into Mongo DB, updates or deletes records based on the metadataIntention in a batch.

5) common/mongo_dao.py
    This class is the Mongo DB access object that takes care DB connection, CRUD operations, and handle DB errors.

6) common/utils.py
    This module provides utility functions such as dumping dictionary to tsv file, json file, extracting exception code and messages.

7) file_validator.py contains:
    A service that polls messages in a specific sqs queue, file_queue, and call data file validator class for validation and duplication checking.
    A class, FileValidator, validates individual data file or data files uploaded for a submission and check duplications.

8) metadata_validator.py contains:
    A service that polls messages in a specific sqs queue, metadata_queue, and call metadata validator class for validation.
    A class, MeataDataValidator, validates metadata node by node and record by record in the node, validation relationships among nodes.

Environment settings:

1) ECR tier:  - key: TIER.  #possible value in ["dev2", "qa"....]
2) STS queues:
    2-1 key: LOADER_QUEUE, for essential validation requests.
    2-2 key: FILE_QUEUE, for data file validation requests.
    2-3 key: METADATA_QUEUE, for metadata validation requests.
3) Mongo database configurations:
    3-1 key: MONGO_DB_HOST, Mongo DB server host.
    3-2 key: MONGO_DB_PORT, Mongo DB server port
    3-3 key: MONGO_DB_USER, Mongo DB server user ID.
    3-4 key: MONGO_DB_PASSWORD, Mongo DB server user password.
    3-5 key: DATABASE_NAME, Mongo database name.
4) Settings in Configuration file and/or arguments:
    4-1 key: service-type  # possible value in ["essential", "file", "metadata"]
    4-2 key: key: models-loc, value: https://raw.githubusercontent.com/CBIIT/crdc-datahub-models/  #only required for service type of essential and metadata.

Usage of the CLI tool:

1) Get helps command:
    $ python src/uploader.py -h

2) Start essential validation service command:
    $ python src/validator.py configs/validator-essential-config.yml

3) Start data file validation service command:
    $ python src/validator.py configs/validator-file-config.yml

4) Start metadata validation service command:
    $ python src/validator.py configs/validator-metadata-config.yml

5) Start data export service command:
    $ python src/validator.py configs/validator-export-config.yml