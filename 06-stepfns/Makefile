.PHONY: build clean deploy

build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/import import/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/preflight preflight/main.go

clean:
	rm -rf ./bin

import-data:
	aws s3 cp ../data.csv s3://infinityworks-ddbimport/data.csv
	aws s3 cp ../data.csv s3://infinityworks-ddbimport/data100k.csv

deploy: clean build
	sls deploy --verbose
