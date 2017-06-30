import re
import ftfy
import string
import unidecode

STOP = [
    "own", "s", "from", "mustn", "whom", "for", "on", "here", "yours", "isn",
    "both", "having", "of", "a", "during", "those", "above", "theirs", "who",
    "won", "just", "d", "about", "haven", "his", "o", "weren", "such", "they",
    "mightn", "can", "hasn", "she", "any", "ll", "then", "nor", "ourselves",
    "what", "t", "its", "needn", "more", "all", "under", "our", "ve", "which",
    "m", "doing", "further", "off", "you", "does", "when", "yourselves", "ain",
    "or", "down", "again", "between", "didn", "doesn", "an", "am", "in", "is",
    "up", "only", "each", "being", "my", "her", "once", "against", "through",
    "did", "was", "with", "do", "him", "before", "few", "myself", "are", "not",
    "why", "been", "will", "this", "them", "ours", "your", "i", "itself", "so",
    "hadn", "where", "very", "and", "to", "as", "had", "most", "into", "no",
    "don", "should", "me", "y", "re", "these", "out", "ma", "hers", "too",
    "he", "now", "by", "yourself", "himself", "has", "after", "than", "couldn",
    "until", "aren", "be", "have", "the", "over", "other", "while", "same",
    "wasn", "herself", "some", "at", "if", "wouldn", "shan", "were", "how",
    "but", "that", "their", "it", "below", "themselves", "because", "shouldn",
    "there", "we"
]

RE_PUNC = re.compile(r'[{}]'.format(re.escape(string.punctuation)))
RE_2LET = re.compile(r'\b[a-z]{1,2}\b')
RE_NUMS = re.compile(r'\b[0-9]+\b')
RE_ALPHA_NUMERIC = re.compile(r'[^a-zA-Z0-9]+')
RE_NONBLANK = re.compile(r'.*[0-9a-zA-Z]+.*')
RE_WORDS_WITH_SYMBOLS = re.compile(r'\b[0-9{}]\b'.format(re.escape(string.punctuation)))


def wordpunct_split(txt):
    splitter = re.compile(
        r"""([{}\s\n\t\r])""".format(re.escape(string.punctuation))
    )
    return [w for w in splitter.split(txt) if w and w != ' ']


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

    words = [i for i in wordpunct_split(txt) if i not in STOP]
    return ' '.join(words)


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

