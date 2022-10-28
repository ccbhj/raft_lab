GO=go
out=./bin

all: raft_cli

raft_cli: init
	${GO} build -o ${out}/$@ main.go

init:
	${GO} mod vendor
clean:
	rm -rf ${out}

.PHONY: clean
