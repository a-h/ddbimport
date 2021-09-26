build:
	cd sls && $(MAKE) package

test:
	cd sls && $(MAKE) statik
	go test ./...

release: 
	if [ "${GITHUB_TOKEN}" == "" ]; then echo "Set the GITHUB_TOKEN environment variable"; fi
	./push-tag.sh
	goreleaser --rm-dist

docker-push: release
	lima nerdctl build -t adrianhesketh/ddbimport:latest .
	pass hub.docker.com/adrianhesketh | lima nerdctl login --username adrianhesketh --password-stdin
	lima nerdctl push adrianhesketh/ddbimport:latest
