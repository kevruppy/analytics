# Use an official Python runtime as a parent image

FROM python:latest

LABEL authors="analytics"

USER root

# Create & activate a virtual environment

RUN python -m venv /opt/analytics/.venv
ENV PATH="/opt/analytics/.venv/bin:$PATH"

# Install additional Python packages

COPY requirements.txt /opt/analytics/requirements.txt
RUN /opt/analytics/.venv/bin/pip install --no-user -r /opt/analytics/requirements.txt

# Set working directory
WORKDIR /opt/analytics