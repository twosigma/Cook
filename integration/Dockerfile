FROM python:3.6

RUN mkdir -p /opt/cook/cli
WORKDIR /opt/cook/cli
COPY cli.tar.gz /opt/cook/cli
RUN tar xzf cli.tar.gz
RUN pip install -e .

RUN mkdir -p /opt/cook/integration
COPY . /opt/cook/integration
WORKDIR /opt/cook/integration
RUN python setup.py install

CMD ["nosetests", "--attr", "!explicit"]
