#!/bin/bash
chown reflinkworker /var/log/reflink-worker.log
chmod 0666 /var/log/reflink-worker.log
chmod +x /etc/init.d/reflink-worker
su -c "$(aws ecr get-login --region=us-east-1)" reflinkworker
service reflink-worker stop || true
service reflink-worker start
