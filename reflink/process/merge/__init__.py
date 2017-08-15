"""
This module is responsible for merging extracted reference metadata.

The :mod:`reflink.process.extract` module provides several reference extraction
mechanisms, each of which provides variable levels of completeness and
quality. Our prior expectations about completeness and quality are represented
by constants in :mod:`.priors`.

.. automodule:: merge
   :members:

"""

from reflink.process.merge import align, arbitrate, beliefs, merge, normalize,\
                                  priors, query
