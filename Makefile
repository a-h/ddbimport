build:
	cd cmd && $(make) build-all
	cd sls && $(make) build

package: build
	cd sls && $(make) package
	
release: package
	# Requires Github CLI (https://cli.github.com/)
	export VERSION=`git rev-list --count HEAD`
	echo Adding git tag with version $VERSION
	git tag v${VERSION}
	git push origin v${VERSION}


