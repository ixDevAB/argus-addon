ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.12-alpine3.21
FROM $BUILD_FROM

# go2rtc carries push-to-talk: the hub's RTSP with its return channel, which Home Assistant's own
# go2rtc never asks for. Pinned to the release the backchannel was verified against.
ARG GO2RTC_VERSION=1.9.14
# iproute2: the per-minute sightings read the neighbour table as JSON, which busybox's ip cannot.
RUN apk add --no-cache curl iproute2 \
    && case "$(uname -m)" in \
         x86_64) go2rtc_arch=amd64 ;; \
         aarch64) go2rtc_arch=arm64 ;; \
         *) echo "unsupported arch $(uname -m)" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /usr/local/bin/go2rtc \
         "https://github.com/AlexxIT/go2rtc/releases/download/v${GO2RTC_VERSION}/go2rtc_linux_${go2rtc_arch}" \
    && chmod +x /usr/local/bin/go2rtc \
    && /usr/local/bin/go2rtc --version

WORKDIR /opt/argus

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY argus_addon/ ./argus_addon/
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD ["/run.sh"]
