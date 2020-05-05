.PHONY: build clean deploy

build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/ddbimport ddbimport/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/preflight preflight/main.go

clean:
	rm -rf ./bin

import-data:
	aws s3 cp ../data.csv s3://infinityworks-ddbimport/data.csv

deploy: clean build
	sls deploy --verbose
