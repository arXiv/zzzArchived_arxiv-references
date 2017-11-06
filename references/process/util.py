"""Helpers for the reference extraction process."""

import re
import os
import shlex
import shutil
import tempfile
import subprocess
import datetime
from typing import List

from references import logging
logger = logging.getLogger(__name__)

VolumeList = List[List[str]]
PortList = List[List[int]]


CATEGORIES = [
    "acc-phys", "adap-org", "alg-geom", "ao-sci", "astro-ph", "atom-ph",
    "bayes-an", "chao-dyn", "chem-ph", "cmp-lg", "comp-gas", "cond-mat", "cs",
    "dg-ga", "funct-an", "gr-qc", "hep-ex", "hep-lat", "hep-ph", "hep-th",
    "math", "math-ph", "mtrl-th", "nlin", "nucl-ex", "nucl-th", "patt-sol",
    "physics", "plasm-ph", "q-alg", "q-bio", "quant-ph", "solv-int",
    "supr-con", "eess", "econ"
]


def files_modified_since(fldr: str, timestamp: datetime.datetime,
                         extension: str = 'pdf') -> list:
    """
    Get a list of files modified since a particular timestamp.

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
        stub, ext = os.path.splitext(filename)
        if ext == '.{}'.format(extension):
            filenames.append(filename)

    return filenames


def find_arxiv_id(string: str) -> str:
    """
    Try to extract an arxiv id from a string.

    Looking for one of two forms:
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


def rotating_backup_name(filename: str) -> str:
    """

    Create the next name in a series of rotating backup files beginning with
    the root name `filename`. In particular, keeping appending `.bk-[0-9]+`
    forever, beginning the first unused number.

    Parameters
    ----------
    filename : str
        Base filename from which to generate backup names

    Returns
    -------
    backup_filename : str
        Name of the next rotating file. If the first time called, it will be
        <filename>.bk-0
    """
    def _genname(base):
        index = 0
        while True:
            yield '{}.bk-{}'.format(base, index)
            index += 1

    for n in _genname(filename):
        if os.path.exists(n):
            continue
        return n
    return None


def backup(filename: str):
    """
    Perform a rotating backup on `filename` in the same directory by appending
    <filename>.bk-[0-9]+

    Parameters
    ----------
    filename : str
        File to back up

    Returns
    -------
    None
    """
    backup_name = rotating_backup_name(filename)
    shutil.copy2(filename, backup_name)


def ps2pdf(ps):
    subprocess.check_call(['ps2pdf', ps])


def dvi2ps(dvi):
    subprocess.check_call(['dvi2ps', dvi])


def argmax(array):
    index, value = max(enumerate(array), key=lambda x: x[1])
    return index
