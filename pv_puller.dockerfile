FROM python:3.11.8-alpine3.19 AS fnl_base_image
 
WORKDIR /usr/validator
COPY . .
RUN pip3 install -r requirements.txt
 
CMD ["/usr/local/bin/python3", "src/validator.py", "configs/pv-puller-config-deploy.yml"]
