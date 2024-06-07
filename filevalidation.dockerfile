#FROM python:3.9.18-alpine3.19
#FROM python:3.11.8-alpine3.19
FROM python:3.11.8-alpine3.19 AS fnl_base_image
 
WORKDIR /usr/validator
COPY . .
RUN pip3 install -r requirements.txt
 
#CMD ["/usr/local/bin/python3", "src/validator.py", "-u", "$MONGO_DB_USER", "-p", "$MONGO_DB_PASSWORD", "-d", "$DATABASE_NAME", "-s", "$MONGO_DB_HOST", "-o", "27017", "-q", "$FILE_QUEUE", "-m", "https://raw.githubusercontent.com/CBIIT/crdc-datahub-models/", "configs/validate-file-config-deploy.yml"]
CMD ["/usr/local/bin/python3", "src/validator.py", "configs/validate-file-config-deploy.yml"]