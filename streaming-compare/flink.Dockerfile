# flink.Dockerfile
FROM flink:1.17.2-scala_2.12

# Install python for PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python || true

# Copy Kafka connector jars
COPY kafka_jars/*.jar /opt/flink/lib/

# Copy YOUR application files (THIS WAS MISSING)
COPY flink/low_watermark_aggregator.py /opt/flink/app/low_watermark_aggregator.py
COPY flink/conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

WORKDIR /opt/flink/app
