FROM python:3.6
MAINTAINER vicervin

RUN  git clone -b cluster-integration --single-branch https://vicervin@github.com/vicervin/dbopt.git

# Creating Application Source Code Directory
#RUN mkdir -p /k8s_python_sample_code/src

# Setting Home Directory for containers
#WORKDIR /k8s_python_sample_code/src

# Installing python dependencies
#COPY requirements.txt /k8s_python_sample_code/src
RUN apt-get update
RUN apt-get install -y swig postgresql-client

RUN pip install numpy==1.16.3
RUN pip install --no-cache-dir -r dbopt/requirements.txt

# Copying src code to Container
#COPY . /k8s_python_sample_code/src/app

# Application Environment variables
#ENV APP_ENV development

# Exposing Ports
#EXPOSE 5035

# Setting Persistent data
#VOLUME ["/app-data"]

# Running Python Application
#CMD ["python", "dbopt/smac_runQueries.py"]

# RUN python dbopt/smac_runQueries.py