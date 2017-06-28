# This Dockerfile describes a basic texlive distribution along with the AutoTeX
# Perl module for automatically creating PDFs / PSs from source. To run, mount
# a host directory to the container's `/autotex` directory:
#
#     docker run -u $UID -v $PWD:/autotex imagename
#
# Either an output will appear or the logfile `auto_gen_ps.log` will contain
# information on what went wrong.
#
# Note that due to licensing we cannot include the AutoTeX source here, you can
# obtain it from CPAN and apply our relevant patches by first running
# prepare-autotex.sh
FROM ubuntu:16.04
ENV VERSION "2015.20160320-1"
LABEL maintainer "mkb72@cornell.edu"
LABEL texlive.version=$VERSION

# install as much of the texlive distribution as we care for at the moment
RUN apt update &&\
    apt install -f -y apt-utils &&\
    apt install -f -y texlive-full=$VERSION &&\
    apt install -f -y perl &&\
    # clean it all up (including removing docs from texlive)
    apt purge -f -y "^texlive-.*-doc" &&\
    apt clean &&\
    apt autoclean -y &&\
    apt clean &&\
    rm -rf /tmp/* /var/tmp/* &&\
    rm -rf /var/lib/apt/lists/*

# clean up all temporary files
RUN mkdir -p /autotex
RUN mkdir -p /perllib

VOLUME ["/autotex/"]

ADD perllib /perllib
ADD scripts /bin

CMD ["autotex.pl"]
