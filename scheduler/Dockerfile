FROM mesosphere/mesos:1.3.0


# Removing docker.list because docker APT repo has been deleted:
# https://www.docker.com/blog/changes-dockerproject-org-apt-yum-repositories/
RUN rm /etc/apt/sources.list.d/docker.list && \
    apt-get -y update && apt-get -y install software-properties-common && \
    sudo apt-get install --reinstall ca-certificates && \
    add-apt-repository ppa:openjdk-r/ppa && apt-get -y update && \
    apt-get --no-install-recommends -y install \
    curl \
    openjdk-11-jdk \
    unzip && apt-get clean && rm -Rf /var/lib/apt/lists/*

# Env setup
ENV HOME "/root/"
ENV LEIN_ROOT true
ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so
ENV JAVA_CMD=/usr/lib/jvm/java-11-openjdk-amd64/bin/java

# Generate SSL certificate
RUN mkdir /opt/ssl
RUN keytool -genkeypair -keystore /opt/ssl/cook.p12 -storetype PKCS12 -storepass cookstore -dname "CN=cook, OU=Cook Developers, O=Two Sigma Investments, L=New York, ST=New York, C=US" -keyalg RSA -keysize 2048

# Lein setup
RUN mkdir $HOME/bin
ENV PATH $PATH:$HOME/bin
RUN curl -o $HOME/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && chmod a+x $HOME/bin/lein && lein

# Create and set the cook dir, copying project file
COPY project.clj /opt/cook/
WORKDIR /opt/cook

# Fetch dependencies
## Only copy the project.clj so that we can use the cached layer
## with fetched dependencies as long as project.clj isn't modified
RUN lein deps

# Datomic setup
COPY datomic /opt/cook/datomic
RUN unzip -uo /opt/cook/datomic/datomic-free-0.9.5561.56.zip

# Copy the whole scheduler into the container
COPY docker /opt/cook/docker
COPY resources /opt/cook/resources
COPY java /opt/cook/java
COPY src /opt/cook/src

RUN lein uberjar
RUN cp "target/cook-$(lein print :version | tr -d '"').jar" datomic-free-0.9.5561.56/lib/cook-$(lein print :version | tr -d '"').jar
COPY config* /opt/cook/

# Ugly hack. Our .cook_kubeconfig lookup assumes it can be found in ../scheduler/ so make a symlink
RUN ln -s /opt/cook /opt/scheduler
COPY .cook_kubeconfig_* /opt/cook/

# Run cook
EXPOSE \
    4334 \
    4335 \
    4336 \
    12321 \
    12322
ENTRYPOINT ["/opt/cook/docker/run-cook.sh"]
CMD ["config.edn"]
