FROM flink:1.17.1-scala_2.12

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev procps && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install apache-flink==1.17.1

# Download official SQL-Kafka connectivity dependencies
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

WORKDIR /app
COPY feature_pipeline.py .