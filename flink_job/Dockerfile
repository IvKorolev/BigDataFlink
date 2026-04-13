FROM flink:1.18.1-scala_2.12-java11

RUN apt-get update -y && apt-get install -y python3 python3-pip wget
RUN pip3 install apache-flink==1.18.1

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

RUN ln -s /usr/bin/python3 /usr/bin/python

COPY flink_processor.py /opt/flink/