build:
	GOOS=linux go build -o ddbimport_linux
	GOOS=windows go build -o ddbimport.exe
	GOOS=darwin go build -o ddbimport_macos

build-docker:
	docker build -t ddbimport:latest .

test-import:
	./ddbimport_macos -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
