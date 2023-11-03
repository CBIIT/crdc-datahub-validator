# crdc-datahub-validator

CRDC datahub Validator is a linux service application for metadata essential validating and lod data to Mongo DB.

The application is programmed purely with python v3.11.  It depends on bento common module, http, json, aws boto3 and so on. All required python modules is listed in the file, requirements.txt, and .gitsubmodules.

The application is consist of multiple python modules/classes to support mutiple functions listed below:

1) Validate local and remote files by verifying md5 and size of files.
2) Create uploading batch via crdc-datahub backend API.
3) Create AWS STS temp credential for uploading files to s3 bucket.
4) Upload both files and metadata files to S3 bucket.
5) Update uploading batch via crdc-datahub backend API.
6) Create uploading report.
7) Log info, error and exceptions.

Major implemented modules/classes in src dir:

1) uploader.py
    This is the entry point of the command line interface.  It controls the workflow and dispatchs different requests to designated modules/classes.

2) upload_config.py
    This class manages request arguments and configurations.  It receives user's arguments, validate these arguments and store them in a dictionary.

3) file_validator.py
    This class validates 1) files by checking file size and md5; 2) metadata files by checking if file existing in the data folder defined by user.
    During the validation, it also constracts a dictionary to record file path, file size, validate result, validateion message for files.  For metadata files, it constructs a dictionary only with file path and file size.

4) common/graphql_client.py
    This class hosts three clients for three graphql APIs, createTempCredential, createBatch, updateBatch.

5) common/s3util.py
    This utility class is for access designated s3 bucket with temp AWS STS credetails, and upload files to the bucket designated by uploading batch.

6) file_uploader.py
    This class manages a job queue for uploading valid files (big size) and metadata files (small size) from local to S3 bucket via copier.py.

7) copier.py
    This class processes each job passed by file-uploader.py, then either upload or put into s3 buket based on upload type, file|metadata, and size via S3util.py.

8) common/utils.py
    This module provides utility fuctions such as dumping dictionry to tsv file, extracting exception code and messages.

Usage of the CLI tool:

1) Get helps command
    $ python src/uploader.py -h
    ##Executing resullts:
    Command line arguments / configuration
    -a --api-url, API endpoint URL, required
    -k --token, API token string, required
    -u --submission, submission ID, required
    -t --type, valid value in [“file”, “metadata”], required
    -d --data, folder that contains either data files (type = “file”) or metadata (TSV/TXT) files (type = “metadata”), required
    -c --config, configuration file path, can potentially contain all above parameters, preferred
    -r --retries, file uploading retries, integer, optional, default value is 3
    Following arguments are needed to read important data from manifest, conditional required when type = “file”

    -m --manifest, path to manifest file, conditional required when type = “file”
    -n --name-field
    -s --size-field
    -m --md5-field
    Following argument is needed when type = "metadata"

    -i --intention, valid value in [“New”, “Update”, “Delete”], conditional required when type = “metadata”, default to “new”
    CLI configuration module will validate and combine parameters from CLI and/or config file
    If config_file is given, then everything else is potentially optional (if it’s included in config file)
    Some arguments are only needed for type = “file” or type = “metadata”, e.g., —intention, —manifest

2) Upload files command
    $ python src/uploader.py -c configs/test-file-upload.yml

3) Upload metadata command
    $ python src/uploader.py -c configs/test-metadata-upload.yml

