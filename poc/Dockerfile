FROM eclipse-temurin:21-jre-jammy

ARG SOLR_VERSION=11.0.0-SNAPSHOT

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl procps iproute2 netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY solr-${SOLR_VERSION}.tgz /tmp/solr.tgz
RUN mkdir -p /opt/solr && \
    tar -xzf /tmp/solr.tgz -C /opt/solr --strip-components=1 && \
    rm /tmp/solr.tgz && \
    useradd -r -u 8983 -g root solr && \
    mkdir -p /var/solr/data /var/solr/logs && \
    chown -R solr:root /opt/solr /var/solr

USER solr
WORKDIR /opt/solr
ENV SOLR_HOME=/var/solr/data
ENV SOLR_LOGS_DIR=/var/solr/logs
ENV SOLR_ULIMIT_CHECKS=false

EXPOSE 8983
ENTRYPOINT ["/opt/solr/bin/solr"]
CMD ["start", "-f"]
