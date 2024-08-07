#FROM python:3.9.18-alpine3.19
FROM python:3.11.8-alpine3.19
 
WORKDIR /usr/validator
COPY . .
RUN pip3 install -r requirements.txt
 
CMD ["/usr/local/bin/python3", "src/validator.py", "configs/validate-export-config-deploy.yml"]
