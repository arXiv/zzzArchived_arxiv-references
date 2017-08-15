#!/bin/bash
touch /var/log/reflink-agent.log
chown reflink-agent /var/log/reflink-agent.log
chmod 0666 /var/log/reflink-agent.log 
service reflink-agent start
