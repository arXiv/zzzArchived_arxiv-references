# Provides a simple HTTP API for Refextract.

FROM python:3.6

RUN apt update && apt install -f -y poppler-utils

ADD requirements.txt /opt/refextract/
ADD wsgi.py /opt/refextract/

RUN pip install -U pip
RUN pip install -r /opt/refextract/requirements.txt

RUN apt-get install -y gcc libpcre3 libpcre3-dev
RUN pip install uwsgi

ENV PATH "/opt/refextract:${PATH}"

# Run test suite instead of starting the application:
ENV CFG_PATH_PDFTOTEXT /usr/bin/pdftotext

RUN mkdir /tmp/uploads
WORKDIR /opt/refextract/

ADD extract /opt/refextract/extract/

CMD uwsgi --http 0.0.0.0:8000 -w wsgi --processes 1 --threads 8
