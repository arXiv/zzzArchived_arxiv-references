#!/bin/bash
chown reflinkworker /var/log/reflink-worker.log
chmod 0666 /var/log/reflink-worker.log
chmod +x /etc/init.d/reflink-worker
service reflink-worker stop || true
service reflink-worker start
