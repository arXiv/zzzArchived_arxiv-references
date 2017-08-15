#!/bin/bash
chown reflink-agent /var/log/reflink-worker.log
chmod 0666 /var/log/reflink-worker.log
service reflink-worker start
