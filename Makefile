GO=go
out=./bin

APP=raft router

all: ${APP}

${APP}: init
	${GO} build -o ${out}/$@ cmd/$@/main.go

init:
	${GO} mod vendor
clean:
	rm -rf ${out}/*

.PHONY: clean ${APP}
