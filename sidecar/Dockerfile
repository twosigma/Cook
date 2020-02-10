FROM python:3.7-alpine

COPY cook /app/cook
COPY Dockerfile /app
COPY README.md /app
COPY setup.py /app
WORKDIR /app
RUN pip3 install -e .
CMD ["cook-sidecar", "8080"]
