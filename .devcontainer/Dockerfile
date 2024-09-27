FROM python:latest

RUN apt-get update && apt-get install -y locales
RUN sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen
RUN locale-gen
ENV LC_ALL=en_US.UTF-8
