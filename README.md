# crdc-datahub-validator

CRDC datahub Validator is a linux service application for validating metadata and file.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3 and so on. All required python modules is listed in the file, requirements.txt, and .gitmodules.

The application is consist of multiple python modules/classes to support multiple functions listed below:

1) Linux services that keeps poll crdc databub message in specific AWS SQS queues for different service types, essential, file and metadata validation services respectively.
2) Metadata model factory that stores models in python dict for different data commons after the service started.
3) Metadata model reader that is called by the metadata model factory to parse data model yaml files.
4) Mongo database access layer for the service to retrieve batch detail, loading metadata into DB and update batch or dataRecords after validation and data loading.
5) File downloader that get metadata file objects from S3 bucket based on batch.
6) Essential validator, file validator and metadata validator to validate file and/or contents.
7) Data loader that update or insert validated data in tsv file into Mongo database.
8) Log info, error and exceptions.

Major implemented modules/classes in src dir:

1) validator.py
    This is the entry point of the command line interface.  It controls the workflow and dispatches to different service based on configured service type.

2) config.py
    This class manages request arguments and configurations.  It receives user's arguments, validate these arguments and store them in a dictionary.

3) essential_validator.py contains:
    A service that poll messages in a specific sqs queue, loader_queue, and call essential validator class for validation and metadata loading.
    A class, EssentialValidator,  validates 1) if batch object contains all required fields; 2) check if files in the batch are metadata; 3) check if tsv file contents are valid. 4) if the batch matadata intention is new, verify no existing records are matched with ids of the data in the files.

4) data_loader.py
    This class loads validated metadata into Mongo DB.

5) common/mongo_dao.py
    This class is the Mongo DB access object that takes care DB connection, CRUD operations, and handle DB errors.

6) common/utils.py
    This module provides utility functions such as dumping dictionary to tsv file, json file, extracting exception code and messages.

7) file_validator.py contains:
    A service that polls messages in a specific sqs queue, file_queue, and call file validator class for validation and duplication checking.
    A class, FileValidator, validates individual file or files uploaded for a submission and check duplications

8) metadata_validator.py contains:
    A service that polls messages in a specific sqs queue, metadata_queue, and call metadata validator class for validation.
    A class, MeataDataValidator, validates metadata node by node and record by record in the node, validation relationships among nodes.

Environment settings:

1) ECR tier:  - key: DEV_TIER.  #possible value in ["dev2", "qa"....]
2) STS queues: 1) key: LOADER_QUEUE 2) key: FILE_QUEUE 3) key: METADATA_QUEUE
3) Settings in Configuration file:
   1.1 Mongo DB connection string: - key: connection-str #e.g. value: mongodb://xxx:xxx@localhost:27017/?authMechanism=DEFAULT
   1.2 Database name: - key: db  #e.g. crdc-datahub2
   1.3 Service type: - key: service-type  # possible value in ["essential", "file", "metadata"]
   1.4 Data model location: - key: models-loc  value: https://raw.githubusercontent.com/CBIIT/crdc-datahub-models/  #only applied for service type of essential    and metadata.
   1.5 Mounted S3 bucket dir: - key: s3_bucket_drive value: /s3_bucket #only applied for service type of file.

Usage of the CLI tool:

1) Get helps command
    $ python src/uploader.py -h
    ##Executing results:
    Command line arguments / configuration

2) Start essential validation service command
    $ python src/validator.py -c configs/validator-metadata-config.yml

3) Start file validation service command
    $ python src/validator.py -c configs/validator-file-config.yml

4) Start metadata validation service command (TBD)

