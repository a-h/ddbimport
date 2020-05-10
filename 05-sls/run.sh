aws lambda invoke \
	--function-name ddbimport-lambda-dev-ddbimport \
	--payload "`cat payload.json`" \
	--log-type Tail output.json
