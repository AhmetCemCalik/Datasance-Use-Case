IMAGE_NAME := accalik/traffic-scheduler
VERSION ?= 0.1.8
PLATFORM := linux/amd64

.PHONY: push clean

push:
	docker buildx build \
		--platform $(PLATFORM) \
		--no-cache \
		-t $(IMAGE_NAME):$(VERSION) \
		--push \
		.

clean:
	docker image rm $(IMAGE_NAME):$(VERSION) || true