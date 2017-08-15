#!/bin/bash

virtualenv /opt/reflink/venv
source /opt/reflink/venv/bin/activate
pip install -r /opt/reflink/arxiv-reflink/requirements.txt
