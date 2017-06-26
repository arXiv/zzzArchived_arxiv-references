"""
The processing pipeline is comprised of several independent modules that perform
specific tasks. These modules are stateless, remain totally unaware of each
other, and are not responsible for IO operations (except reading from/writing
to the filesystem). Each module exposes a single method.

Retriever:
- Expects an arXiv ID;
- Retrieves the PDF and TeX sources, and stores them on the filesystem;
- Returns the location of the PDF and TeX sources on the filesystem.
"""


from reflink.tasks.retrieve.fake import fake_retrieve as retrieve
