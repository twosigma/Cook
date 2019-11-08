FROM python:3.7-alpine

COPY . /app
WORKDIR /app
RUN pip3 install -e .
CMD ["fileserver", "8080"]