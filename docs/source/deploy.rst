Deployment
**********

Worker (service)
================

- AMI: arxiv-references-worker-image
- Auto Scaling Group: arxiv-references-worker
- cp deploy/config/appspec-worker.yml appspec.yml
- zip -r ../arxiv-references-worker-{version}.zip -@ < deploy/package.txt
- rm appspec.yml

Agent
=====

- AMI: arxiv-references-agent-image
- Auto Scaling Group: arxiv-references-agent
- cp deploy/config/appspec-agent.yml appspec.yml
- zip -r ../arxiv-references-agent-{version}.zip -@ < deploy/package.txt
- rm appspec.yml

API (frontend)
==============

- ElasticBeanstalk arxiv-references-api
- zip -r ../arxiv-references-api-{version}.zip -@ < deploy/package.txt
