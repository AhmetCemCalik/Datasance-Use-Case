FROM alpine:3.22

ARG NATS_C_VERSION=v3.11.0

RUN apk add --no-cache \
    build-base \
    cmake \
    git \
    openssl-dev

WORKDIR /tmp

RUN git clone \
        --branch "${NATS_C_VERSION}" \
        --depth 1 \
        https://github.com/nats-io/nats.c.git \
        /tmp/nats.c

WORKDIR /tmp/nats.c/build

RUN cmake .. \
        -DNATS_BUILD_STREAMING=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build . \
        --target nats nats_static \
        --parallel \
    && cmake --install .

RUN test -f /usr/local/include/nats/nats.h \
    && test -n "$(find /usr/local/lib -name 'libnats.so*' -print -quit)"

WORKDIR /workspace