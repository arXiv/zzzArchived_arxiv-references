"""Prior expectations about field-values and extractor field quality."""

EXTRACTORS = [
    (
        'refextract',
        {
            '__all__': 1.0,
            'authors': 0.5,
            'raw': 0.8,
            'issue': 0.6,
            'source': 1.0,
        }
    ),
    (
        'cermine',
        {
            '__all__': 1.0,
            'authors': 0.9,
            'raw': 1.0,
            'issue': 0.9,
            'source': 0.9,
        }
    ),
    (
        'grobid',
        {
            '__all__': 1.0,
            'authors': 1.0,
            'raw': 0.8,
            'issue': 0.8,
            'source': 0.9,
        }
    ),
    (
        'scienceparse',
        {
            '__all__': 1.0,
            'authors': 0.9,
            'raw': 0.8,
            'issue': 0.8,
            'source': 0.9,
        }
    ),
]
