FROM mesosphere/mesos:1.3.0

RUN apt-get -y update
RUN apt-get -y install curl

# Env setup
ENV HOME "/root/"
ENV LEIN_ROOT true
ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

# Lein setup
RUN mkdir $HOME/bin
ENV PATH $PATH:$HOME/bin
RUN curl -o $HOME/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
RUN chmod a+x $HOME/bin/lein
RUN lein

# Create and set the cook dir, copying project file
COPY project.clj /opt/cook/
WORKDIR /opt/cook

# Fetch dependencies
## Only copy the project.clj so that we can use the cached layer
## with fetched dependencies as long as project.clj isn't modified
RUN lein deps

# Copy the whole scheduler into the container
COPY . /opt/cook/

# Run cook
EXPOSE 12321
ENTRYPOINT ["lein", "with-profiles", "+docker", "run"]
CMD ["config.edn"]
