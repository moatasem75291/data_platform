FROM python:3.12-slim-bookworm
ARG ICEBERG_VERSION=1.12.0
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PYTHONUNBUFFERED=1 MPLBACKEND=Agg
WORKDIR /opt/lab
COPY requirements.in requirements.lock.txt /opt/
RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -r /opt/requirements.lock.txt \
    && python -m pip freeze > /opt/requirements.lock.txt
RUN mkdir -p /opt/jars && \
    curl -fL --retry 3 -o /opt/jars/iceberg-runtime.jar https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -fL --retry 3 -o /opt/jars/iceberg-aws.jar https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    curl -fL --retry 3 -o /opt/jars/postgresql.jar https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar
CMD ["bash", "scripts/start-notebook.sh"]
