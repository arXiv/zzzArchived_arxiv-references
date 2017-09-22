#!/bin/bash

rm -rf /opt/reflink/arxiv-reflink || true
rm /etc/init.d/reflink-worker || true
rm /etc/init.d/reflink-agent || true
rm /opt/reflink/bin/start_worker || true
rm /opt/reflink/bin/start_kcl || true
rm /opt/reflink/bin/agent.properties || true
rm -rf /opt/reflink/venv || true
