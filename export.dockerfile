FROM python:3.14.4-alpine3.23 AS fnl_base_image

RUN apk upgrade --no-cache
 
WORKDIR /usr/validator
COPY . .
RUN pip3 install -r requirements.txt
 
CMD ["/usr/local/bin/python3", "src/validator.py", "configs/validate-export-config-deploy.yml"]
