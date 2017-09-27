# arxiv/references-agent

FROM ubuntu:zesty

RUN apt-get update
RUN apt-get -y update
RUN apt-get install -y ca-certificates wget
RUN apt-get install -y default-jdk

# Install Python 3.6 and friends.
RUN apt-get install -y libpython3.6 python3.6 python3.6-dev python3.6-venv
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3.6 get-pip.py

#RUN ln -s /usr/local/bin/pip /usr/local/bin/pip3


RUN apt-get install -y build-essential autoconf libtool pkg-config gcc g++

# Add Python consumer and configuration.
ADD references /opt/arxiv/references/
ADD agent.properties /opt/arxiv/
ADD test.properties /opt/arxiv/
ADD requirements.txt /opt/arxiv/
ADD run_consumer.py /opt/arxiv/
ADD start_agent.sh /opt/arxiv/
RUN pip install -U pip
RUN pip install -r /opt/arxiv/requirements.txt

ENV PATH "/opt/arxiv:/opt/arxiv/jars:${PATH}"

# Runtime configuration.
ENV AWS_ACCESS_KEY_ID ""
ENV AWS_SECRET_ACCESS_KEY ""
# Set to `test` when testing with localstack.
ENV MODE "agent"

# Set `-Dcom.amazonaws.sdk.disableCertChecking` for testing
ENV JAVA_FLAGS ""

# Set `true` when testing with localstack.
ENV AWS_CBOR_DISABLE false

# If not set, secrets will not be retrieved.
ENV SECRETS_BUCKET_NAME ""
ENV ARXIV_HOME "https://arxiv.org"

RUN chmod +x /opt/arxiv/start_agent.sh

RUN ln -s /usr/bin/python3.6 /usr/local/bin/python3
RUN ln -s /usr/local/bin/python3 /usr/local/bin/python
CMD ["/bin/bash", "/opt/arxiv/start_agent.sh"]
#CMD ["/bin/bash"]
