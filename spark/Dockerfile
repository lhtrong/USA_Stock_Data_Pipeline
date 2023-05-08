FROM bitnami/spark

USER root

RUN apt-get update -qq && apt-get install vim -qqq&& apt-get install -y wget
ENV HOME=/opt/bitnami/spark

RUN mkdir spark \
    && cd spark \
    && wget https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz\
    && tar xzfv openjdk-11.0.1_linux-x64_bin.tar.gz \
    && rm openjdk-11.0.1_linux-x64_bin.tar.gz
ENV JAVA_HOME="${HOME}/spark/jdk-11.0.1" 
ENV PATH="${JAVA_HOME}/bin:${PATH}" 

RUN cd spark \
    && wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz \
    && tar xzfv spark-3.4.0-bin-hadoop3.tgz \
    && rm spark-3.4.0-bin-hadoop3.tgz 

ENV SPARK_HOME="${HOME}/spark/spark-3.4.0-bin-hadoop3" 
ENV PATH="${SPARK_HOME}/bin:${PATH}"

ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"


# Downloading gcloud package
RUN wget https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz -O /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


RUN gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar ${HOME}/spark/spark-3.4.0-bin-hadoop3/jars


COPY ./app ./app

COPY ./credentials ./credentials
ENV GOOGLE_APPLICATION_CREDENTIALS=$HOME/credentials/service-account-key.json


COPY ../requirements.txt /
RUN pip install -r /requirements.txt





