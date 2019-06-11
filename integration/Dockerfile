FROM ubuntu:18.10

RUN apt-get update
RUN apt-get install -y python3.6 python3-pip

RUN apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common \
    apt-utils

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
RUN apt-get update
RUN apt-get install -y docker-ce-cli

WORKDIR /opt/cook/integration
COPY requirements.txt /opt/cook/integration
ADD cli.tar.gz /opt/cook/cli/
RUN pip3 install -r requirements.txt

# Don't need to copy over the integration test files --- they're bind-mounted.
ENTRYPOINT ["pytest"]