"""Test the Dockerized RefExtract extractor application."""

from unittest import TestCase
import os
import subprocess
import requests

from arxiv import status
import json


WORKDIR = os.path.split((os.path.abspath(__file__)))[0]


class TestRefExtractExtractor(TestCase):
    """Build, run, and test the Dockerized RefExtract extractor."""

    @classmethod
    def setUpClass(cls):
        """Build the docker image and start it."""
        build = subprocess.run(
            "docker build . -t arxiv/refextract",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=True, cwd=WORKDIR
        )
        if build.returncode != 0:
            raise RuntimeError(
                "Failed to build image: %s" % build.stdout.decode('ascii')
            )
        print('Built Docker image')

        start = subprocess.run(
            "docker run -d -p 8912:8000 arxiv/refextract",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=True, cwd=WORKDIR
        )
        if start.returncode != 0:
            raise RuntimeError(
                "Failed to start: %s" % start.stdout.decode('ascii')
            )
        cls.container = start.stdout.decode('ascii').strip()
        print(f'Started RefExtract extract as {cls.container}')

    @classmethod
    def tearDownClass(cls):
        """Tear down the container once all tests have run."""
        stop = subprocess.run(f"docker rm -f {cls.container}",
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              shell=True, cwd=WORKDIR)
        if stop.returncode != 0:
            raise RuntimeError(
                "Failed to stop container: %s" % stop.stdout.decode('ascii')
            )
        print('Stopped container; done.')

    def test_extract(self):
        """Pass a real arXiv PDF for extraction."""
        pdf_path = os.path.join(WORKDIR, 'extract/tests/data/1704.01689v1.pdf')
        with open(pdf_path, 'rb') as f:
            response = requests.post(
                'http://localhost:8912/refextract/extract',
                files={'file': f}
             )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.headers['content-type'], 'application/json')
        try:
            data = json.loads(response.content)
        except json.decoder.JSONDecodeError:
            self.fail("Should return valid JSON")
        self.assertGreater(len(data), 0, "Should return a list of references")
