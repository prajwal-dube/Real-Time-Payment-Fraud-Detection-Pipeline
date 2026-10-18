FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=/usr/local/bin/python
WORKDIR /app
COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor/ .
RUN python train.py --output /app/artifacts
CMD ["spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6","/app/stream.py"]
