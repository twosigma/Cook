FROM mesosphere/mesos:1.0.1-rc1

RUN apt-get -y update
RUN apt-get -y install curl

# Env setup
ENV HOME "/root/"
ENV LEIN_ROOT true

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

# Lein Setup
RUN mkdir $HOME/bin
ENV PATH $PATH:$HOME/bin
RUN curl -o $HOME/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
RUN chmod a+x $HOME/bin/lein
RUN lein

RUN mkdir -p /opt/cook
WORKDIR /opt/cook
COPY project.clj /opt/cook/
RUN lein deps

COPY . /opt/cook

EXPOSE 12321
ENTRYPOINT ["lein", "run"]
CMD ["container-config.edn"]
