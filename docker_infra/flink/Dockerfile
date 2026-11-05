FROM flink:1.18

# install python + JDK
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    openjdk-11-jdk \
    build-essential \
    curl \
    && ln -s /usr/bin/python3 /usr/bin/python

# set JAVA_HOME (QUAN TRỌNG)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# upgrade pip tools
RUN pip3 install --upgrade pip setuptools wheel

# install PyFlink
RUN pip3 install apache-flink==1.18.1

# add kafka connector + client
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
