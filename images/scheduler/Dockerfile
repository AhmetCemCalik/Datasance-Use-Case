FROM accalik/traffic-nats-c-base:3.11.0 AS builder

WORKDIR /src

COPY scheduler.c .

RUN cc \
    -std=c11 \
    -O2 \
    -Wall \
    -Wextra \
    -Wpedantic \
    scheduler.c \
    -I/usr/local/include \
    -L/usr/local/lib \
    -lnats \
    -o junction-scheduler

FROM alpine:3.22

RUN apk add --no-cache \
        libssl3 \
        libcrypto3 \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app

COPY --from=builder /src/junction-scheduler /app/junction-scheduler
COPY --from=builder /usr/local/lib/libnats.so* /usr/local/lib/

USER app

ENTRYPOINT ["/app/junction-scheduler"]