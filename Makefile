GOPATH=`pwd`/../../../../
GO=GOPATH=$(GOPATH) go
BIN=$(GOPATH)bin/mineshaft

OK_COLOR=\033[32;01m
NO_COLOR=\033[0m

build:
	@echo "$(OK_COLOR)==>$(NO_COLOR) Installing dependencies"
	@$(GO) get -v ./...
	@echo "$(OK_COLOR)==>$(NO_COLOR) Compiling"
	@$(GO) build -o $(BIN) -v cmd/mineshaft.go

run: build
	@echo "$(OK_COLOR)==>$(NO_COLOR) Running"
	$(BIN) -f=mineshaft.conf

sync:
	rsync -avzP --delete --exclude=.git --exclude=.DS_Store ../../../ matt.i.disqus.net:mineshaft/src

.PHONY: build run sync
