FROM accalik/traffic-nats-c-base:3.11.0 AS builder

WORKDIR /src

COPY light.c .

RUN cc \
    -std=c11 \
    -O2 \
    -Wall \
    -Wextra \
    -Wpedantic \
    light.c \
    -I/usr/local/include \
    -L/usr/local/lib \
    -lnats \
    -o traffic-light

FROM alpine:3.22

RUN apk add --no-cache \
        libssl3 \
        libcrypto3 \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app

COPY --from=builder /src/traffic-light /app/traffic-light
COPY --from=builder /usr/local/lib/libnats.so* /usr/local/lib/

USER app

ENTRYPOINT ["/app/traffic-light"]