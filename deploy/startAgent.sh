#!/bin/bash
touch /var/log/reflink-agent.log
chown reflinkconsumer /var/log/reflink-agent.log
chmod 0666 /var/log/reflink-agent.log
chmod +x /etc/init.d/reflink-agent
service reflink-agent stop || true
service reflink-agent start
