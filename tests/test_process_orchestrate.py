"""Tests for :mod:`reflink.process.orchestrate` module."""

import unittest
from reflink.process.orchestrate import tasks


# class TestOrchestrate(unittest.TestCase):
#     """Test :func:`.tasks.process_document` with a real arXiv document."""
#
#     def setUp(self):
#         """Given an arXiv document ID..."""
#         self.document_id = '1602.00026'
#
#     def test_process_document(self):
#         """Test that the entire processing workflow completes successfully."""
#
#         try:
#             tasks.process_document(self.document_id)
#         except Exception as e:
#             self.fail(e.msg)
