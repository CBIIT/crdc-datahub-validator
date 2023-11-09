# crdc-datahub-validator

CRDC datahub Validator is a linux service application for validating metadata and file.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3 and so on. All required python modules is listed in the file, requirements.txt, and .gitmodules.

The application is consist of multiple python modules/classes to support multiple functions listed below:

1) A Linux service that keeps poll crdc databub message in a specific AWS SQS queue by utilizing bento SQS utils.
2) Metadata model factory that stores model yaml files for different data commons after the service started.
3) Metadata model reader that is called by the metadata model factory to parse data model yaml files.
4) Mongo database access layer for the service to retrieve batch detail, loafing metadata into DB anf update batch after validation and data loading.
5) File downloader that get metadata file objects from S3 bucket based on batch.
6) Essential validator, file validator and metadata validator to validate file contents.
7) Data loader that update or insert validated data in tsv file into Mongo database.
8) Log info, error and exceptions.

Major implemented modules/classes in src dir:

1) validator.py
    This is the entry point of the command line interface.  It controls the workflow and dispatches different requests to designated modules/classes.

2) config.py
    This class manages request arguments and configurations.  It receives user's arguments, validate these arguments and store them in a dictionary.

3) essential_validator.py
    This class validates 1) if batch object contains all required fields; 2) check if files in the batch are metadata; 3) check if tsv file contents are valid. 4) if the batch matadata intention is new, verify no existing records are matched with ids of the data in the files.

4) data_loader.py
    This class loads validated metadata into Mongo DB.

5) common/mongo_dao.py
    This class is the Mongo DB access object that takes care DB connection, CRUD operations, and handle DB errors.

6) common/utils.py
    This module provides utility functions such as dumping dictionary to tsv file, json file, extracting exception code and messages.

Usage of the CLI tool:

1) Get helps command
    $ python src/uploader.py -h
    ##Executing results:
    Command line arguments / configuration

2) Validate metadata command
    $ python src/validator.py -c configs/validator-metadata-config.yml

