#!/bin/bash

virtualenv --python=/usr/bin/python3 /opt/reflink/venv
source /opt/reflink/venv/bin/activate
pip install -r /opt/reflink/arxiv-reflink/requirements.txt
