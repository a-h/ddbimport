.PHONY: build clean deploy

build:
	env GOOS=linux go build -ldflags="-s -w -X github.com/a-h/ddbimport/log.v=`git rev-list --count HEAD`" -o bin/import import/main.go
	env GOOS=linux go build -ldflags="-s -w -X github.com/a-h/ddbimport/log.v=`git rev-list --count HEAD`" -o bin/preflight preflight/main.go

clean:
	rm -rf ./bin

deploy: clean build
	sls deploy --verbose

statik:
	mkdir -p ./.serverless
	go get github.com/rakyll/statik
	statik -src=./.serverless

package: clean build
	sls package
	$(MAKE) statik
