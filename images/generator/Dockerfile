FROM accalik/traffic-nats-c-base:3.11.0 AS builder

WORKDIR /src

COPY generator.c .

RUN cc \
    -std=c11 \
    -O2 \
    -Wall \
    -Wextra \
    -Wpedantic \
    generator.c \
    -I/usr/local/include \
    -L/usr/local/lib \
    -lnats \
    -o traffic-generator

FROM alpine:3.22

RUN apk add --no-cache \
        libssl3 \
        libcrypto3 \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app

COPY --from=builder /src/traffic-generator /app/traffic-generator
COPY --from=builder /usr/local/lib/libnats.so* /usr/local/lib/

USER app

ENTRYPOINT ["/app/traffic-generator"]