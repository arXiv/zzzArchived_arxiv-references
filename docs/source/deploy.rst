Deployment
**********

This will get the whole system up and running locally, for dev/testing
purposes. Before you start, be sure to up your memory allocation for the
Docker VM. Grobid is the greediest of all, and will exit on OOM. I use 8GB
on my machine, but you could probably get away with less.

.. code-block:: bash

   # First, build everything!
   git clone git@github.com:cul-it/arxiv-references.git
   cd arxiv-reflink
   docker-compose build

   # We also need redis, localstack:
   docker pull redis
   docker pull localstack

   docker-compose up

The references API should be available on ``http://localhost:8001``.

To run e2e tests (after everything is up):

.. code-block:: bash
   docker build ./ -t references-test -f ./Dockerfile-test
   docker run -it --network=arxivreferences_references-test references-test

That's it! Try:

POST to ``http://localhost:8000/references`` with a JSON payload:

.. code-block:: javascript

   {
        "document_id": "1606.00123v3",
        "url": "https://arxiv.org/pdf/1606.00123v3.pdf"
   }

Use `boto3
<http://boto3.readthedocs.io/en/latest/reference/services/kinesis.html>`_ to
put a new record on the ``PDFIsAvailable`` stream. Use the same payload as
above.
