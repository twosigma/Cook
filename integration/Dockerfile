FROM python:3.6

WORKDIR /opt/cook/integration
COPY requirements.txt /opt/cook/integration
ADD cli.tar.gz /opt/cook/cli/
RUN pip install -r requirements.txt

# Don't need to copy over the integration test files --- they're bind-mounted.
ENTRYPOINT ["pytest"]