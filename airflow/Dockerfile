FROM apache/airflow:latest-python3.9

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install vim -qqq

RUN apt-get update && apt-get upgrade -y && apt-get install wget

RUN mkdir ${AIRFLOW_HOME}/spark \
    && cd ${AIRFLOW_HOME}/spark \
    && wget https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz\
    && tar xzfv openjdk-11.0.1_linux-x64_bin.tar.gz \
    && rm openjdk-11.0.1_linux-x64_bin.tar.gz


RUN cd ${AIRFLOW_HOME}/spark \
    && wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz \
    && tar xzfv spark-3.4.0-bin-hadoop3.tgz \
    && rm spark-3.4.0-bin-hadoop3.tgz 



ENV JAVA_HOME="${AIRFLOW_HOME}/spark/jdk-11.0.1" 
ENV PATH="${JAVA_HOME}/bin:${PATH}" 

ENV SPARK_HOME="${AIRFLOW_HOME}/spark/spark-3.4.0-bin-hadoop3" 
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# # Downloading gcloud package
# RUN wget https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz -O /tmp/google-cloud-sdk.tar.gz

# # Installing the package
# RUN mkdir -p /usr/local/gcloud \
#   && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
#   && /usr/local/gcloud/google-cloud-sdk/install.sh

# # Adding the package path to local
# ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -P ${AIRFLOW_HOME}/spark/spark-3.4.0-bin-hadoop3/jars 



COPY --chown=airflow:airflow ./credentials ./credentials
RUN chmod -R 765 ${AIRFLOW_HOME}/credentials/service-account-key.json
ENV GOOGLE_APPLICATION_CREDENTIALS=${AIRFLOW_HOME}/credentials/service-account-key.json

USER airflow

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
