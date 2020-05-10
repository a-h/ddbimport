build:
	GOOS=linux go build -o ddbimport_linux
	GOOS=windows go build -o ddbimport.exe
	GOOS=darwin go build -o ddbimport_macos

build-docker:
	docker build -t ddbimport:latest .
