build:
	cd cmd && $(MAKE) build-all
	cd sls && $(MAKE) build

package: build
	cd sls && $(MAKE) package

release: 
	if [ "${GITHUB_TOKEN}" == "" ]; then echo "Set the GITHUB_TOKEN environment variable"; fi
	./push-tag.sh
	goreleaser --rm-dist
