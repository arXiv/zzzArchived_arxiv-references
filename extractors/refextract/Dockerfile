# Provides a simple HTTP API for Refextract.

FROM python:3.6

RUN apt-get update && apt-get install -f -y \
    poppler-utils \
    gcc \
    libpcre3 \
    libpcre3-dev \
 && rm -rf /var/lib/apt/lists/*

ADD requirements.txt /opt/refextract/
ADD wsgi.py /opt/refextract/

RUN pip install -U pip
RUN pip install -r /opt/refextract/requirements.txt

RUN pip install uwsgi

ENV PATH "/opt/refextract:${PATH}"

# Run test suite instead of starting the application:
ENV CFG_PATH_PDFTOTEXT /usr/bin/pdftotext

RUN mkdir /tmp/uploads
WORKDIR /opt/refextract/

ADD extract /opt/refextract/extract/

CMD uwsgi --http-socket :8000 -w wsgi -t 3000 --processes 8 --threads 1 -M --async 100 --ugreen
