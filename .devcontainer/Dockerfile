FROM python:latest

# Install necessary packages
RUN apt-get update && apt-get install -y locales

# Edit /etc/locale.gen to enable the desired locale
RUN sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen

# Generate the locale
RUN locale-gen

# Set the default locale
ENV LC_ALL en_US.UTF-8
