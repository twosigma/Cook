FROM python:3.5

RUN apt-get update
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"

RUN apt-get update
RUN apt-get -y install docker-ce

RUN mkdir -p /opt/cook/integration
COPY . /opt/cook/integration
WORKDIR /opt/cook/integration
RUN python setup.py install

CMD ["python", "setup.py", "nosetests"]