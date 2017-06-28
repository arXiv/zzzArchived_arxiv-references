import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

import re
import os
import shlex
import shutil
import tempfile
import subprocess
import datetime
from contextlib import contextmanager
from typing import List

VolumeList = List[List[str]]

@contextmanager
def indir(directory: str) -> None:
    """
    Context manager for performing actions within a given directory, guaranteed
    to return to previous directory upon exit.

    Parameters
    ----------
    directory: str

    Returns
    -------
    None
    """
    cwd = os.getcwd()
    try:
        os.chdir(directory)
        yield
        os.chdir(cwd)
    except Exception as e:
        raise e
    finally:
        os.chdir(cwd)

@contextmanager
def tempdir(cleanup: bool = True) -> str:
    """
    A near copy of tempfile.TemporaryDirectory but does not clean up
    automatically after calling. Useful for debugging purposes for troublesome
    pdfs in the workflow.

    Parameters
    ----------
    cleanup: bool
        Whether to delete the directory after use

    Returns
    -------
    directory: str
        Temporary directory name
    """
    directory = tempfile.mkdtemp()
    try:
        yield directory
    except Exception as e:
        raise e
    finally:
        if cleanup:
            shutil.rmtree(directory)

def run_docker(image: str, volumes: VolumeList = [], args: str = '') -> (str, str):
    """
    Run a generic docker image. In our uses, we wish to set the userid to that
    of running process (getuid) by default. Additionally, we do not expose
    any ports for running services making this a rather simple function.

    Parameters
    ----------
    image : str
        Name of the docker image in the format 'repository/name:tag'

    volumes : list of tuple of str
        List of volumes to mount in the format [host_dir, container_dir]. 

    args : str
        Arguments to the image's run cmd (set by Dockerfile CMD)
    """
    # we are only running strings formatted by us, so let's build the command
    # then split it so that it can be run by subprocess
    opt_user = '-u {}'.format(os.getuid())
    opt_volumes = ' '.join([
        '-v {}:{}'.format(host_dir, container_dir) for host_dir, container_dir in volumes
    ])
    cmd = 'docker run --rm {} {} {} {}'.format(opt_user, opt_volumes, image, args)
    cmd = shlex.split(cmd)

    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        logger.error(
            "Docker image call '{}' returned error {}".format(
                ' '.join(cmd), result.returncode
            )
        )
        logger.error("STDOUT: {}\nSTDERR: {}".format(result.stdout, result.stderr))
        result.check_returncode()

    return result 

def files_modified_since(fldr: str, timestamp: datetime.datetime,
        extension: str = 'pdf'):
    """
    Get a list of files modified since a particular timestamp, which also match
    the given extension.

    Parameters
    ----------
    fldr : str
        Directory in which to recursively look for files

    timestamp : datetime.datetime
        Timestamp to use for modification break point

    extension : str
        Extension of files to search for

    Returns
    -------
    filenames : list of str
        Filenames of those modified
    """
    thelist = []
    for root, dirs, files in os.walk(fldr):  
        for fname in files:
            path = os.path.join(root, fname)
            st = os.stat(path)    
            mtime = datetime.datetime.fromtimestamp(st.st_mtime)
            if mtime > timestamp:
                thelist.append(fname)

    filenames = []
    for filename in thelist:
        stub, ext = os.path.splitext(f)
        if ext == '.{}'.format(extension):
            filenames.append(filename)

    return filenames

def find_arxiv_id(string: str) -> str:
    """
    Try to extract an arxiv id from a string, looking for one of two forms:
        1. New form -- 1603.00324
        2. Old form -- hep-th/0002839

    Parameters
    ----------
    string : str
        String in which to perform the search

    Returns
    -------
    id : str
        The arxiv id found in the string, '' if none found.
    """
    oldform = re.compile(r'([a-z\-]{4,8}\/\d{7})')
    newform = re.compile(r'(\d{4,}\.\d{5,})')

    for regex in (newform, oldform):
        match = regex.findall(string)
        if len(match) > 0:
            return match[0]
    return ''

def ps2pdf(ps):
    subprocess.check_call(['ps2pdf', ps])

def dvi2ps(dvi):
    subprocess.check_call(['dvi2ps', dvi])


