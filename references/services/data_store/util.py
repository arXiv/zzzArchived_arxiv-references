"""Helpers for data store."""


def clean(reference: dict) -> dict:
    """
    Remove empty values.

    Parameters
    ----------
    reference : dict

    Returns
    -------
    dict
    """
    if reference is None:
        return

    # TODO: This will not work if we want an explicitly False(y) value.
    def _inner_clean(datum):
        if isinstance(datum, dict):
            return {k: _inner_clean(v) for k, v in datum.items() if v}
        elif isinstance(datum, list):
            return [_inner_clean(v) for v in datum if v]
        return datum

    return {field: _inner_clean(value) for field, value
            in reference.items() if value}
