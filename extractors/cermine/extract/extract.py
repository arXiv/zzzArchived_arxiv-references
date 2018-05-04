"""Provides CERMINE integration for reference extraction."""

import os
import subprocess
import shlex
import tempfile
from arxiv.base import logging

logger = logging.getLogger(__name__)


def extract_with_cermine(filepath: str) -> str:
    """
    Perform reference extraction with CERMINE.

    Runs CERMINE as a Java subprocess, and returns the XML result.

    Parameters
    ----------
    filepath : str
        Location of a PDF on disk.

    Returns
    -------
    str
        XML output from CERMINE.

    """
    basepath, filename = os.path.split(filepath)
    stub, ext = os.path.splitext(os.path.basename(filename))

    cmd = shlex.split("java -cp /opt/cermine/cermine-impl.jar"
                      " pl.edu.icm.cermine.ContentExtractor"
                      " -path %s" % basepath)

    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.debug('%s: Cermine exit code: %i' % (filename, r.returncode))
        logger.debug('%s: Cermine stdout: %s' % (filename, r.stdout))
        logger.debug('%s: Cermine stderr: %s' % (filename, r.stderr))
    except subprocess.CalledProcessError as e:
        raise RuntimeError('CERMINE failed: %s' % filename) from e

    outpath = os.path.join(basepath, '{}.cermxml'.format(stub))
    if not os.path.exists(outpath):
        raise RuntimeError('%s not found, expected output' % outpath)
    try:
        with open(outpath, 'rb') as f:
            result = f.read()
    except Exception as e:
        raise IOError('Could not read Cermine output at %s: %s' %
                      (outpath, e)) from e
    return result
