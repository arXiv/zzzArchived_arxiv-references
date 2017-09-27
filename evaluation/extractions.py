import sys
sys.path.append('.')
from unittest import mock
from references.process import extract
from references.process.merge import align, arbitrate, beliefs, normalize, priors
import os
from pprint import pprint
import csv
from references import logging
logging.getLogger('references.process.extract').setLevel(40)

basepath = os.path.abspath('evaluation/pdfs')

if __name__ == '__main__':
    with open('evaluation/referenceCounts.csv') as f:
        raw = [row for row in csv.reader(f)]

    referenceCounts = [{k: row[i] for i, k in enumerate(raw[0])}
                       for row in raw if len(row) == len(raw[0])]

    for row in referenceCounts:

        full_path = os.path.join(basepath, row['pdf'])
        if not os.path.exists(full_path):
            continue
        document_id = row['pdf'][:-4]
        print('Extracting %s' % document_id)

        extractions = extract.extract(full_path, document_id, report=False)
        for extractor, refs in extractions.items():
            print(extractor, len(refs), row['N'])

        N_extractions = len(extractions)
        aligned_records = align.align_records(extractions)

        print('aligned', len(aligned_records), row['N'])

        aligned_probabilities = beliefs.validate(aligned_records)
        arbitrated_records = arbitrate.arbitrate_all(aligned_records,
                                                     aligned_probabilities,
                                                     priors.EXTRACTORS,
                                                     N_extractions)
        final_records, score = normalize.filter_records(arbitrated_records)
        print('final', len(final_records), row['N'], score)
        print('--')
