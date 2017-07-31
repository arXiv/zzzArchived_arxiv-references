"""Prior expectations about field-values and extractor field quality."""

EXTRACTORS = [
    (
        'refextract',
        {
            'title': 0.9,
            'doi': 0.6,
        }
    ),
]



# TODO: This is just a place-holder; should be replaced with something more
#  realistic.
def validate_title(title: str) -> float:
    """
    Calculate the probability of ``title`` given our prior beliefs.

    Parameters
    ----------
    title : str

    Returns
    -------
    float
        Probability that ``title`` is a correct extraction of the title of a
        reference.
    """
    return 0.1 if len(title) < 5 or len(title) > 500 else 0.9



FIELDS = {
    'title': validate_title,
}
