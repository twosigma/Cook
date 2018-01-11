FROM python:3.6

ADD cli.tar.gz /opt/cook/cli/
COPY . /opt/cook/integration
WORKDIR /opt/cook/integration
RUN pip install -r requirements.txt

CMD ["pytest"]
