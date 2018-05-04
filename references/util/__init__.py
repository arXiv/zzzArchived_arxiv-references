"""Helpers for the reference extraction process."""

import re
import os
import shlex
import shutil
import tempfile
import subprocess
import datetime
from typing import List, Optional, Generator

from arxiv.base import logging
logger = logging.getLogger(__name__)

VolumeList = List[List[str]]
PortList = List[List[int]]


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
        match: List[str] = regex.findall(string)
        if len(match) > 0:
            return match[0]
    return ''


def rotating_backup_name(filename: str) -> str:
    """
    Create the next name in a series of rotating backup files.

    Beginning with the root name `filename`. In particular, keeping appending
    `.bk-[0-9]+` forever, beginning the first unused number.

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
    def _genname(base: str) -> Generator:
        index = 0
        while True:
            yield '{}.bk-{}'.format(base, index)
            index += 1

    for n in _genname(filename):
        if os.path.exists(n):
            continue
        break
    return n    # type: ignore


def backup(filename: str) -> None:
    """
    Perform a rotating backup on `filename` in the same directory.

    Works by appending <filename>.bk-[0-9]+

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


def ps2pdf(ps: str) -> None:
    """Wrapper for ps2pdf."""
    subprocess.check_call(['ps2pdf', ps])


def dvi2ps(dvi: str) -> None:
    """Wrapper for dvi2ps."""
    subprocess.check_call(['dvi2ps', dvi])


def argmax(array: List[float]) -> int:
    """Simple argmax implementation for lists of floats."""
    index, value = max(enumerate(array), key=lambda x: x[1])
    return index
