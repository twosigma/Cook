FROM python:3.5

RUN mkdir -p /opt/cook/integration
COPY . /opt/cook/integration
WORKDIR /opt/cook/integration
RUN python setup.py install

CMD ["python", "setup.py", "nosetests"]