import unittest
import shlex
import subprocess
import requests
import os
import time
import json
from datetime import datetime, timedelta
import decimal


def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


GROBID_IMAGE = '626657773168.dkr.ecr.us-east-1.amazonaws.com/arxiv/grobid:latest'


class ReferenceExtractionAndArbitration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pull = """docker pull %s""" % GROBID_IMAGE
        pull = shlex.split(pull)
        start = """docker run -p 80:8080 -e "http.host=0.0.0.0" -e "transport.host=127.0.0.1" %s""" % GROBID_IMAGE
        start = shlex.split(start)
        subprocess.run(pull, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.Popen(start, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        time.sleep(10)
        start_time = datetime.now()
        while True:
            try:
                r = requests.get('http://127.0.0.1/processFulltextDocument')
            except IOError as e:
                continue
            if r.status_code == 405:
                break
            # Try for a minute, then bail.
            if datetime.now() - start_time > timedelta(seconds=60):
                raise RuntimeError('Failed to start ES in reasonable time')
            time.sleep(1)

    def setUp(self):
        os.environ['GROBID_HOSTNAME'] = 'localhost'
        os.environ['GROBID_PORT'] = '80'

    def test_extraction(self):
        from references.process import extract
        from references.process.merge import align, arbitrate, priors, beliefs, \
            normalize
        # refs = extract.extract('evaluation/pdfs/0801.0012.pdf',
        #                        document_id="0801.0012", report=False)
        pdf_path = 'evaluation/pdfs/0801.0012.pdf'
        document_id = '0801.0012'

        extractions = {}
        extractions['cermine'] = extract.cermine.extract_references(
                                    pdf_path, document_id)
        extractions['grobid'] = extract.grobid.extract_references(
                                    pdf_path, document_id)
        extractions['refextract'] = extract.refextract.extract_references(
                                        pdf_path, document_id)

        # with open('data/0801.0012.cermine.json', 'w') as f:
        #     json.dump(extractions['cermine'], f, indent=4, default=decimal_default)
        # with open('data/0801.0012.grobid.json', 'w') as f:
        #     json.dump(extractions['grobid'], f, indent=4, default=decimal_default)
        # with open('data/0801.0012.refextract.json', 'w') as f:
        #     json.dump(extractions['refextract'], f, indent=4, default=decimal_default)

        extractions = {extractor: normalize.normalize_records(extracted)
                       for extractor, extracted in extractions.items()}
        # with open('data/0801.0012.normalized.json', 'w') as f:
        #     json.dump(extractions, f, indent=4, default=decimal_default)

        aligned_records = align.align_records(extractions)
        # with open('data/0801.0012.aligned.json', 'w') as f:
        #     json.dump(aligned_records, f, indent=4, default=decimal_default)
        aligned_probabilities = beliefs.validate(aligned_records)
        # with open('data/0801.0012.probabilities.json', 'w') as f:
        #     json.dump(aligned_probabilities, f, indent=4, default=decimal_default)
        arbitrated_records = arbitrate.arbitrate_all(aligned_records,
                                                     aligned_probabilities,
                                                     priors.EXTRACTORS, 3)
        # with open('data/0801.0012.arbitrated.json', 'w') as f:
        #     json.dump(arbitrated_records, f, indent=4, default=decimal_default)
        final_records, score = normalize.filter_records(arbitrated_records)
        # with open('data/0801.0012.final.json', 'w') as f:
        #     json.dump(final_records, f, indent=4, default=decimal_default)


    @classmethod
    def tearDownClass(cls):
        """Spin down ES."""
        ps = "docker ps | grep %s | awk '{print $1;}'" % GROBID_IMAGE
        container = subprocess.check_output(["/bin/sh", "-c", ps]) \
            .strip().decode('ascii')
        stop = """docker stop %s""" % container
        stop = shlex.split(stop)
        subprocess.run(stop, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
