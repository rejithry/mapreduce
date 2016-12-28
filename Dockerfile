FROM sequenceiq/hadoop-docker:2.7.1

MAINTAINER Rejith

USER root

RUN yum clean all; \
    rpm --rebuilddb; \
    yum install -y git

ENV MAVEN_VERSION 3.2.5

# Download and install Maven
RUN curl -sSL http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar xzf - -C /usr/share \
&& mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
&& ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ADD test.txt /
ADD url.txt /
ADD build.sh /
RUN chmod 755 /build.sh

CMD ["/etc/bootstrap.sh", "-d"]

