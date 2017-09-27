"""Evaluation metrics for reference extraction."""

import os
import json
import sys
from statistics import mean, stdev
sys.path.append('.')

from references.process import extract, merge


if __name__ == '__main__':

    basepath = os.path.abspath(os.path.join('evaluation'))
    pdfs = list(filter(lambda fname: fname.endswith('.pdf'),
                os.listdir(os.path.join(basepath, 'pdfs'))))
    pdf_path = os.path.join(basepath, 'pdfs', pdfs[2])
    document_id = pdfs[2][:-4]
    truth_path = os.path.join(basepath, 'truth', '%s.json' % document_id)
    with open(truth_path) as f:
        truth = json.load(f)

    extracted = (extract.refextract.extract_references(pdf_path, document_id))

    aligned = merge.align.align_records({
        'extracted': extracted,
        'truth': truth
    })
    scores = []

    for extractions in aligned:
        if len(extractions) != 2:
            scores.append(0.)
            continue
        extractions = dict(extractions)
        extraction_t = extractions['truth']
        extraction_e = extractions['extracted']
        fields = ((set(extraction_t.keys()) |
                   set(extraction_e.keys()))
                  - {'reftype'})
        field_scores = []
        for field in fields:
            value_t = extraction_t.get(field, None)
            value_e = extraction_e.get(field, None)
            v_sim = merge.arbitrate._similarity(value_t, value_e)
            field_scores.append(v_sim)
        scores.append(mean(field_scores))
    print(mean(scores), stdev(scores))
