import unittest
from unittest import mock


class TestExtract(unittest.TestCase):
    # @mock.patch('reflink.process.extract.scienceparse.extract_references')
    # @mock.patch('reflink.process.extract.cermine.extract_references')
    # @mock.patch('reflink.process.extract.grobid.extract_references')
    @mock.patch('reflink.process.extract.getDefaultExtractors')
    @mock.patch('reflink.process.retrieve.retrieve')
    def test_extract(self, mock_retrieve, mock_get_extractors, *others):

        pdf_path = 'tests/data/1702.07336.pdf'
        mock_retrieve.return_value = (pdf_path, None)
        mock_extractor = mock.MagicMock()
        mock_get_extractors.return_value = [
            ('test', mock_extractor)
        ]


        from reflink.process.tasks import process_document
        process_document('1702.07336')
        self.assertEqual(mock_retrieve.call_count, 1)
        self.assertEqual(mock_extractor.call_count, 1)
        self.assertEqual(mock_extractor.call_args, pdf_path)
