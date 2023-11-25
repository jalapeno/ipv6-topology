REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all ipv6-topology container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: ipv6-topology

ipv6-topology:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-ipv6-topology

ipv6-topology-container: ipv6-topology
	docker build -t $(REGISTRY_NAME)/ipv6-topology:$(IMAGE_VERSION) -f ./build/Dockerfile.ipv6-topology .

push: ipv6-topology-container
	docker push $(REGISTRY_NAME)/ipv6-topology:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
