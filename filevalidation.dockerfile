FROM python:3.14.4-alpine3.23 AS fnl_base_image

RUN apk upgrade --no-cache

WORKDIR /usr/validator
COPY . .
RUN pip3 install -r requirements.txt
 
#CMD ["/usr/local/bin/python3", "src/validator.py", "-u", "$MONGO_DB_USER", "-p", "$MONGO_DB_PASSWORD", "-d", "$DATABASE_NAME", "-s", "$MONGO_DB_HOST", "-o", "27017", "-q", "$FILE_QUEUE", "-m", "https://raw.githubusercontent.com/CBIIT/crdc-datahub-models/", "configs/validate-file-config-deploy.yml"]
CMD ["/usr/local/bin/python3", "src/validator.py", "configs/validate-file-config-deploy.yml"]