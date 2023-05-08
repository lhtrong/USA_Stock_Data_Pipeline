FROM apache/airflow:2.3.1-python3.8

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq

RUN apt-get update && apt-get upgrade -y && apt-get install wget
RUN wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz \
 && tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz -C /usr/lib/jvm/
RUN apt-get autoremove -yqq --purge 
RUN apt-get clean `
RUN rm -rf /var/lib/apt/lists/*
RUN rm -rf openjdk-11.0.2_linux-x64_bin.tar.gz 
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/jdk-11.0.2
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3