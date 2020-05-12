build: build-mac

build-all: build-mac build-linux build-windows build-docker

build-mac:
	GOOS=darwin go build -o ddbimport_macos

build-linux:
	GOOS=linux go build -o ddbimport_linux

build-windows:
	GOOS=windows go build -o ddbimport.exe

build-docker: build-linux
	docker build -t ddbimport:latest .

test-import-remote: build
	./ddbimport_macos -remote -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport

test-import-local: build
	./ddbimport_macos -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport

test-import-local-file: build
	./ddbimport_macos -inputFile ../data.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
