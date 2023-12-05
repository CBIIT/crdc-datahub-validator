FROM python:3.9.18-alpine3.18
 
WORKDIR /usr/validator
COPY . .
RUN pip3 install -r requirements.txt
 
CMD [/usr/local/bin/python3 src/validator.py -c configs/validator-metadata-config.yml]