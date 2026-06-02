ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.21
FROM $BUILD_FROM

WORKDIR /opt/argus

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY argus_addon/ ./argus_addon/
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
