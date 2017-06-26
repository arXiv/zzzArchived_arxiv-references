"""
The processing pipeline is comprised of several independent modules that perform
specific tasks. These modules are stateless, remain totally unaware of each
other, and are not responsible for IO operations (except reading from/writing
to the filesystem). Each module exposes a single method.

Link injector:
- Expects the location of a TeX source bundle on the filesystem, and structured
  metadata returned by the extractor;
- Injects reference links into the TeX source, and calls the TeX processor to
  generate a PDF;
- Returns the location of the new link-injected PDF on the filesystem.
"""

from .fake import fake_inject as inject
