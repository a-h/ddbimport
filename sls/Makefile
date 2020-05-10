.PHONY: build clean deploy

build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/import import/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/preflight preflight/main.go

clean:
	rm -rf ./bin

deploy: clean build
	sls deploy --verbose
