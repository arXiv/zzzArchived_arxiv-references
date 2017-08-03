"""Service layer integration for CERMINE."""

import os
import shutil
import subprocess
from reflink.services import util
from flask import _app_ctx_stack as stack


class ExtractionError(Exception):
    """Encountered an unexpected state during extraction."""

    pass


class CermineSession(object):
    """Represents a configured Cermine session."""

    def __init__(self, image: str) -> None:
        """
        Set the Docker image for Cermine.

        Parameters
        ----------
        image : str
        """
        self.image = image
        try:
            # TODO: docker pull to verify image is available.
            pass
        except Exception as e:
            raise IOError('Failed to pull Cermine image %s: %s' %
                          (self.image, e)) from e

    def extract_references(self, filename: str, cleanup: bool=False):
        """
        Extract references from the PDF represented by ``filehandle``.

        Parameters
        ----------
        filename : str

        Returns
        -------
        str
            Raw XML response from Cermine.
        """
        fldr, name = os.path.split(filename)
        stub, ext = os.path.splitext(os.path.basename(filename))

        with util.tempdir(cleanup=cleanup) as tmpdir:
            # copy the pdf to our temporary location
            tmppdf = os.path.join(tmpdir, name)
            shutil.copyfile(filename, tmppdf)

            try:
                # FIXME: magic string for cermine container
                util.run_docker(self.image, [[tmpdir, '/pdfs']])
            except subprocess.CalledProcessError as e:
                raise ExtractionError('CERMINE failed: %s' % filename) from e

            cxml = os.path.join(tmpdir, '{}.cermxml'.format(stub))
            if not os.path.exists(cxml):
                raise FileNotFoundError('%s not found, expected output' % cxml)
            try:
                with open(cxml) as f:
                    return f.read()
            except Exception as e:
                raise IOError('Could not read Cermine output at %s: %s' %
                              (cxml, e)) from e


class Cermine(object):
    """Cermine integration from reflink worker application."""

    def __init__(self, app=None):
        """Set and configure the current application instance, if provided."""
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        """Configure an application instance."""
        app.config.setdefault('REFLINK_CERMINE_DOCKER_IMAGE', 'arxiv/cermine')

    def get_session(self) -> CermineSession:
        """Generate a new configured :class:`.CermineSession`."""
        try:
            image = self.app.config['REFLINK_CERMINE_DOCKER_IMAGE']
        except (RuntimeError, AttributeError) as e:   # No application context.
            image = os.environ.get('REFLINK_CERMINE_DOCKER_IMAGE',
                                   'arxiv/cermine')
        return CermineSession(image)

    @property
    def session(self):
        """Get or creates a :class:`.CermineSession` for the current app."""
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'cermine'):
                ctx.cermine = self.get_session()
            return ctx.cermine
        return self.get_session()     # No application context.
