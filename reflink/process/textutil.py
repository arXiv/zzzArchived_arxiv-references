import re
import ftfy
import string
import unidecode
import unicodedata

def normalize_unicode(txt):
    return unicodedata.normalize('NFKD', txt).encode('ascii','ignore').decode('ascii')

def normalize_unicode2(txt):
    return unidecode.unidecode(txt)

from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize

import numpy as np

stop = set(stopwords.words('english'))
RE_PUNC = re.compile(r'[{}]'.format(re.escape(string.punctuation)))
RE_2LET = re.compile(r'\b[a-z]{1,2}\b')
RE_NUMS = re.compile(r'\b[0-9]+\b')
RE_ALPHA_NUMERIC = re.compile(r'[^a-zA-Z0-9]+')
RE_NONBLANK = re.compile(r'.*[0-9a-zA-Z]+.*')
RE_WORDS_WITH_SYMBOLS = re.compile(r'\b[0-9{}]\b'.format(re.escape(string.punctuation)))

def not_blank(txt):
    return RE_NONBLANK.match(txt) is not None

def remove_latex(txt):
    return re.subn(r"(\$.*?\$)", '', txt)[0]

def clean_text(txt):
    """ Removes stop words, punctuation, makes lower case """
    txt = normalize_unicode(txt)
    txt = txt.lower()
    txt = remove_latex(txt)
    txt = RE_PUNC.subn(' ', txt)[0]
    txt = RE_2LET.subn(' ', txt)[0]
    txt = RE_NUMS.subn(' ', txt)[0]
    txt = RE_ALPHA_NUMERIC.subn(' ', txt)[0]

    words = [i for i in wordpunct_tokenize(txt) if i not in stop]
    return ' '.join(words)

def re_contains(words, flags=re.IGNORECASE, dirty=True):
    # look-ahead assertion, contains all words in any order
    f = lambda w: r'(?=.*\b{}\b)'.format(w)
    a = ''.join([f(w) for w in words])
    return re.compile(r"^"+a+r".+", flags=flags)

def re_atleast(words, flags=re.IGNORECASE, dirty=True):
    return re.compile(r"({})".format('|'.join([re.escape(w) for w in words])), re.I)


punctuation_pat = re.compile(r"""([!"#$%&\'()*+,-./:;<=>?@[\\\]^_`{|}~])""")
hyphenline_pat = re.compile(r"-\s*\n\s*")
multiwhite_pat = re.compile(r"\s+")
cid_pat = re.compile(r"\(cid:\d+\)")
nonlet = re.compile(r"([^A-Za-z0-9 ])")
purenum = re.compile(r"\b[0-9]+\b")

def clean_text(txt, numok=False):
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

def clean_blob(blob, numok=False):
    output = []

    lines = blob.split('\n')
    for line in lines:
        txt = ftfy.fix_text(line, normalization='NFKC')
        txt = unidecode.unidecode(txt)
        txt = clean_text(txt, numok=numok)
        output.append(txt)
    return '\n'.join(output)


def remove_latex_markup(txt):
    tfile0 = '/tmp/pandoc.input'
    tfile1 = '/tmp/pandoc.output'

    with open(tfile0, 'w') as f:
        f.write(txt)

    try:
        line = 'pandoc -f latex -t plain -o'
        line = shlex.split(line)
        line += [str(i) for i in [tfile1, tfile0]]
        check_call(line)
    except Exception as e:
        return txt

    txt = open(tfile1).read()
    txt = re.subn('\n', ' ', txt)[0]
    return txt
