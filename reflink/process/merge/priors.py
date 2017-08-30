"""Prior expectations about field-values and extractor field quality."""

EXTRACTORS = [
    (
        'refextract',
        {
            '__all__': 1.0,
            'authors': 0.5,
        }
    ),
    (
        'cermine',
        {
            '__all__': 1.0,
        }
    ),
    (
        'grobid',
        {
            '__all__': 1.0,
        }
    ),
    (
        'scienceparse',
        {
            '__all__': 1.0,
        }
    ),
]
