"""
The processing pipeline is comprised of several independent modules that perform
specific tasks. These modules are stateless, remain totally unaware of each
other, and are not responsible for IO operations (except reading from/writing
to the filesystem). Each module exposes a single method.

Extractor:
- Expects the location of a PDF on the filesystem;
- Extracts reference lines and structured reference metadata;
- Returns reference lines and metadata.
"""

from reflink.tasks.extract.fake import fake_extract as extract
