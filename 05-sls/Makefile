.PHONY: build clean deploy

build:
	env GOOS=linux go build -ldflags="-s -w" -o bin/ddbimport ddbimport/main.go

clean:
	rm -rf ./bin

import-data:
	aws s3 cp ../data.csv s3://infinityworks-ddbimport/data.csv

run-lambda:
	aws lambda invoke --function-name arn:aws:lambda:eu-west-2:386676700885:function:ddbimport-dev-ddbimport --payload "`cat payload.json`" --log-type Tail output.json

deploy: clean build
	sls deploy --verbose
