"""Text cleanup utilities."""

import re
import ftfy
import unidecode

punctuation_pat = re.compile(r"""([!"#$%&\'()*+,-./:;<=>?@[\\\]^_`{|}~])""")
hyphenline_pat = re.compile(r"-\s*\n\s*")
multiwhite_pat = re.compile(r"\s+")
cid_pat = re.compile(r"\(cid:\d+\)")
nonlet = re.compile(r"([^A-Za-z0-9 ])")
purenum = re.compile(r"\b[0-9]+\b")


def clean_text(txt, numok = False):
    """
    Normalize a set of text so that it can be compared with different sources.

    Potentially with different encodings and varying whitespace etc.
    """
    txt = txt.lower()
    txt = cid_pat.sub(" UNK ", txt)
    txt = hyphenline_pat.sub("", txt)

    txt = punctuation_pat.sub(r" ", txt)
    txt = nonlet.sub(r" ", txt)

    if not numok:
        txt = purenum.sub(r" ", txt)

    txt = multiwhite_pat.sub(" ", txt)
    txt = txt.encode('utf-8').decode('utf-8')
    return txt.strip()


def clean_blob(blob, numok = False):
    """
    Given a blob of text, apply the `clean_text` to each line in the text.
    """
    output = []

    lines = blob.split('\n')
    for line in lines:
        txt = ftfy.fix_text(line, normalization='NFKC')
        txt = unidecode.unidecode(txt)
        txt = clean_text(txt, numok=numok)
        output.append(txt)
    return '\n'.join(output)
