Architecture
============

This document describes the architecture of the reference linking service,
using a loose interpretation of the
[C4 architectural model](https://www.structurizr.com/help/c4).

Note: the term "container" in this document should not be conflated with
Docker containers; the two concepts are not related.

Context
-------
The reference extraction feature is a stand-alone service deployed in
the AWS cloud environment. The legacy arXiv system will issue notifications
about new publications via a message broker. The message broker will in turn
notify the reference linking service. For each new publication, the reference
linking service will retrieve published PDFs and LaTeX source packages,
extract and parse cited references, and produce a new link-injected PDF.

The legacy arXiv system, and/or other consumers, can subsequently request
reference metadata or the location of the link-injected PDF for individual
arXiv publications via a REST API.

.. image:: static/images/context.png

Containers
----------

Since reference extraction is time-consuming (several minutes per document),
we isolate the reference extraction process from the REST API. The reference
extraction process is responsible for processing notifications from the
notification broker, extracting and storing reference metadata, and producing
link-injected PDFs. This will be deployed on one or more dedicated EC2 virtual
machines. The REST API is deployed separately as a WSGI web application running
on ElasticBeanstalk.

Those two containers have access to a shared object store (S3) and a shared
data store (DynamoDB). The REST API will only *read* from those object stores,
and the reference extraction process will only *write* to those object stores.

.. image:: static/images/containers.png

Components
----------

.. image:: static/images/components.png

Reference extraction
````````````````````
The reference extraction process is comprised of two main macro-components: a
notification-handling process that listens for new publications, and an
asynchronous task pipeline for processing individual documents.

Notification handling is provided by two components: a notification consumer
provided by Amazon, implemented using the Java-based Kinesis Consumer
Library, and a record processor component implemented in Python that
processes new notifications received by the consumer. A so-called
MultiLangDaemon, a stand-alone Java process, provides the glue between the
KCL and our record processor. When new notifications are received by the
consumer, the MultiLangDaemon invokes the record processor, which in turn
starts the processing pipeline. See :mod:`reflink.notification.consumer` for
details.

The entry point to the processing pipeline is the orchestrate component, which
generates a series of tasks (more or less in series) for each arXiv publication.
See :mod:`reflink.process.orchestrate` for details. Those tasks -- retrieve,
extract, link inject -- are executed asynchronously. See
:mod:`reflink.process.retrieve`\, :mod:`reflink.process.extract`\, and
:mod:`reflink.process.inject`\. Asynchronous task management is performed by
Celery, which relies on an external message broker. The orchestrate component
is also responsible for storing the extracted references and link-injected PDF.
See :mod:`reflink.process.store`\.

Access to the data store (for reference metadata) and object store (for link
injected PDFs) are provided by corresponding service components that expose a
very simple CRUD API. Those service components are used by both the reference
extraction process and the web application. See
:mod:`reflink.services.data_store` and :mod:`reflink.services.object_store` for
details.

REST API
`````````
The REST API is provided by a web application, implemented in Flask, running
on AWS ElasticBeanstalk. This uses a fairly straightforward MVC pattern;
blueprints and view functions (:mod:`reflink.web.views`\) arbitrate client
requests and serialize responses, lightweight controllers
(:mod:`reflink.web.controllers`\) are responsible for interpreting request
content and retrieving relevant data from the data store and object store, and
the :mod:`reflink.services.data_store` and :mod:`reflink.services.object_store`
service components provide access to the underlying DynamoDB and S3 services.
