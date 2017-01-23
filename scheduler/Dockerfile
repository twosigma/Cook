FROM clojure

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
    echo "deb http://repos.mesosphere.com/debian jessie main" | tee /etc/apt/sources.list.d/mesosphere.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y --force-yes mesos && \
    apt-get clean

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

RUN mkdir -p /opt/cook
WORKDIR /opt/cook
COPY project.clj /opt/cook/
RUN lein deps

COPY . /opt/cook

EXPOSE 12321
ENTRYPOINT ["/opt/cook/bin/launch-cook.sh"]
CMD ["container-config.edn"]
