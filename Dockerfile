FROM eclipse-temurin:17-jdk-jammy

ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin

RUN apt-get update && \
    apt-get install -y curl python3 python3-pip && \
    pip3 install pyspark==3.5.1 && \
    pip3 install matplotlib && \
    pip3 install pandas && \
    rm -rf /var/lib/apt/lists/*

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

WORKDIR /app
COPY app/ /app/

CMD ["spark-submit", "--master", "local[*]", "main.py"]
