GOPATH=`pwd`/../../../../
GOBIN=$(GOPATH)bin/
GO=GOPATH=$(GOPATH) GOBIN=$(GOBIN) go
APPS=\
	mineshaft\
	mineshaft-bench

OK_COLOR=\033[32;01m
NO_COLOR=\033[0m

build:
	@for app in $(APPS); do \
		echo "$(OK_COLOR)==>$(NO_COLOR) Building $${app}"; \
		echo "$(OK_COLOR)==>$(NO_COLOR) Installing dependencies"; \
		$(GO) get -v cmd/$${app}.go; \
		echo "$(OK_COLOR)==>$(NO_COLOR) Compiling"; \
		$(GO) install -v cmd/$${app}.go; \
	done;

run: build
	@echo "$(OK_COLOR)==>$(NO_COLOR) Running"
	$(GOBIN)mineshaft -f=mineshaft.conf

test:
	$(GO) test -v ./...

clean:
	rm -rf $(GOBIN)*

.PHONY: build run test clean
