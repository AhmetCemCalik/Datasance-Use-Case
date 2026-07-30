FROM accalik/traffic-nats-c-base:3.11.0 AS builder

WORKDIR /src

COPY observer.c .

RUN cc \
    -std=c11 \
    -O2 \
    -Wall \
    -Wextra \
    -Wpedantic \
    observer.c \
    -I/usr/local/include \
    -L/usr/local/lib \
    -lnats \
    -o traffic-observer


FROM alpine:3.22

RUN apk add --no-cache \
        libssl3 \
        libcrypto3 \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app

COPY --from=builder \
    /usr/local/lib/libnats.so* \
    /usr/local/lib/

COPY --from=builder \
    /src/traffic-observer \
    /app/traffic-observer

USER app

ENTRYPOINT ["/app/traffic-observer"]