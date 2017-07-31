def merge_records(records: List[List[dict]]) -> dict:
    """
    Takes a list of reference metadata records (each formatted according to the
    schema) and reconciles them with each other to form one primary record for
    each item. First step is to match the lists against each other using
    similarity measures. Then, for each individual record we combine the
    possible fields and augment them with possible external information to form
    a single record.

    Parameters
    ----------
    records : dictionary of list of reference metadata
        The reference records from multiple extraction servies / lookup services.
        Each is labelled as {'extractor': [references]}

    Returns
    -------
    united : list of dict (reference data)
    """
    matched_records = []
