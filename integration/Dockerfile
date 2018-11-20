FROM python:3.6

WORKDIR /opt/cook/integration
COPY requirements.txt /opt/cook/integration
ADD cli.tar.gz /opt/cook/cli/
RUN pip install -r requirements.txt
COPY . /opt/cook/integration


ENTRYPOINT ["pytest"]