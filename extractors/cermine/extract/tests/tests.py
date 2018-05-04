"""Unit tests for :mod:`extract`."""

from unittest import TestCase, mock
import tempfile
import os
import io
import subprocess

from arxiv import status
from extract import routes
from extract.factory import create_cermine_app
from extract.extract import extract_with_cermine

DATA_PATH = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'data')


class TestExtract(TestCase):
    """Test :mod:`extract.extract`."""

    @classmethod
    def setUpClass(cls):
        """Load some sample CERMINE output."""
        # extract_with_cermine expects the input and output files to be in the
        # same location.
        cls.id = '1704.01689v1'
        cls.workdir = tempfile.mkdtemp()
        with open(os.path.join(DATA_PATH, f'{cls.id}.cermxml')) as f:
            cls.raw = f.read()
        with open(os.path.join(cls.workdir, f'{cls.id}.cermxml'), 'w') as f:
            f.write(cls.raw)

        # A fake path to a PDF in the working directory.
        cls.filepath = os.path.join(cls.workdir, f'{cls.id}.pdf')

    @mock.patch('extract.extract.subprocess')
    def test_extract_with_cermine(self, mock_subprocess):
        """extract_with_cermine() is called with an extractable PDF."""
        out = extract_with_cermine(self.filepath)
        self.assertEqual(out.decode('utf-8'), self.raw,
                         "Should return raw CERMINE XML output.")

    @mock.patch('extract.extract.subprocess')
    def test_subprocess_raises_error(self, mock_subprocess):
        """Subprocess module raises a CalledProcessError."""
        mock_subprocess.CalledProcessError = subprocess.CalledProcessError

        def raise_calledprocesserror(*args, **kwargs):
            raise subprocess.CalledProcessError(1, 'foocommand')
        mock_subprocess.run.side_effect = raise_calledprocesserror

        with self.assertRaises(RuntimeError):
            extract_with_cermine(self.filepath)

    @mock.patch('extract.extract.subprocess')
    def test_cermine_produces_no_output(self, mock_subprocess):
        """CERMINE produces no XML output."""
        # Will look in the same directory as the PDF, so we pass a path
        # for which the basedirectory does not exist.
        filepath = os.path.join(self.workdir + 'foo', f'{self.id}.pdf')

        with self.assertRaises(RuntimeError):
            extract_with_cermine(filepath)

    @mock.patch('extract.extract.open')
    @mock.patch('extract.extract.subprocess')
    def test_cannot_open_cermine_output(self, mock_subprocess, mock_open):
        """CERMINE produces output, but it cannot be read."""
        mock_open.side_effect = IOError

        with self.assertRaises(IOError):
            extract_with_cermine(self.filepath)


class TestRoutes(TestCase):
    """Tests for :mod:`.routes`."""

    @mock.patch('extract.routes.request')
    def test_extract_without_file(self, mock_request):
        """:func:`.routes.extract` is called with no files."""
        mock_request.files = {}

        content, status_code = routes.extract()
        self.assertEqual(status_code, status.HTTP_400_BAD_REQUEST,
                         "Should return 400 Bad Request")

    @mock.patch('extract.routes.request')
    def test_extract_with_non_pdf(self, mock_request):
        """:func:`.routes.extract` is passed something other than a PDF."""
        mock_filestorage = mock.MagicMock()
        mock_filestorage.filename = 'foo.exe'
        mock_request.files = {'file': mock_filestorage}

        content, status_code = routes.extract()
        self.assertEqual(status_code, status.HTTP_400_BAD_REQUEST,
                         "Should return 400 Bad Request")

    @mock.patch('extract.routes.cleanup_upload')
    @mock.patch('extract.routes.extract_with_cermine')
    @mock.patch('extract.routes.current_app')
    @mock.patch('extract.routes.request')
    def test_extract_with_pdf(self, mock_request, mock_app, mock_extract,
                              mock_cleanup):
        """:func:`.routes.extract` is passed something other than a PDF."""
        mock_filestorage = mock.MagicMock()
        mock_filestorage.filename = 'foo.pdf'
        mock_request.files = {'file': mock_filestorage}
        mock_app.config = {}
        mock_extract.return_value = 'fooxml'

        content, status_code = routes.extract()

        self.assertEqual(mock_extract.call_count, 1,
                         "extract_with_cermine should be called")
        self.assertTrue(mock_extract.call_args[0][0].endswith('foo.pdf'),
                        "Path to uploaded file should be passed to"
                        " extract_with_cermine")
        self.assertEqual(content.content_type, 'application/xml',
                         "Response should have content type application/xml")
        self.assertEqual(status_code, status.HTTP_200_OK,
                         "Should return 200 OK")
        self.assertEqual(mock_cleanup.call_count, 1,
                         "Should remove both the PDF and the CERMINE output.")


class TestApp(TestCase):
    """Test the extractor application as a whole."""

    def setUp(self):
        """Initialize the application."""
        self.app = create_cermine_app()
        self.app.config['UPLOAD_PATH'] = self.workdir
        self.client = self.app.test_client()

    @classmethod
    def setUpClass(cls):
        """Load some sample CERMINE output."""
        # extract_with_cermine expects the input and output files to be in the
        # same location.
        cls.id = '1704.01689v1'
        cls.workdir = tempfile.mkdtemp()
        with open(os.path.join(DATA_PATH, f'{cls.id}.cermxml')) as f:
            cls.raw = f.read()
        os.mkdir(os.path.join(cls.workdir, cls.id))
        outfile = os.path.join(cls.workdir, f'{cls.id}/{cls.id}.cermxml')
        with open(outfile, 'w') as f:
            f.write(cls.raw)

        # A fake path to a PDF in the working directory.
        cls.filepath = os.path.join(cls.workdir, f'{cls.id}.pdf')

    @mock.patch('extract.extract.subprocess')
    def test_request_extract(self, mock_subprocess):
        """A POST request is made to /extract."""
        data = {'file': (io.BytesIO(b'foopdf'), f'{self.id}.pdf')}
        response = self.client.post('/cermine/extract', data=data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data.decode('utf-8'), self.raw,
                         "Should return CERMINE XML output.")
        
