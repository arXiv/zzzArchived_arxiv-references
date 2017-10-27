#!/bin/bash

uwsgi --http 0.0.0.0:8000 -w wsgi --processes 1 --threads 8 --manage-script-name
