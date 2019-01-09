FROM python:3.6

RUN pip install flask

COPY . /opt/cook/integration
ENV FLASK_APP /opt/cook/integration/service.py

EXPOSE 5131

ENTRYPOINT ["flask", "run", "--host=0.0.0.0", "--port=5131"]
