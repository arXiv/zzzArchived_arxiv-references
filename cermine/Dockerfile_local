# Dockerfile for the CERMINE runner jar. Very simple, only needs to place the jar
# in a container and set up a few locations for running it sucessfully. The jar
# is not contained in this repository, but can be found at:
#
#     http://maven.ceon.pl/artifactory/simple/kdd-releases/pl/edu/icm/cermine/cermine-impl/
# 
# In particular, this Dockerfile uses the 1.12 jar with full dependences. To run,
# mount a host directory to /pdfs inside the container, i.e.
# 
#     docker run --rm -u $UID -v $HOSTPATH:/pdfs imagename
#
# The directory will be populated with the .cermxml files for each pdf in the
# directory.
FROM anapsix/alpine-java
ENV VERSION "1.12"
LABEL maintainer "mkb72@cornell.edu"
LABEL cermine.version $VERSION

COPY cermine-impl-$VERSION-jar-with-dependencies.jar /cermine-$VERSION-deps.jar
VOLUME ["/pdfs"]

CMD ["sh", "-c", "java -cp /cermine-${VERSION}-deps.jar pl.edu.icm.cermine.ContentExtractor -path /pdfs"]
