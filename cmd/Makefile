test-import-remote: 
	go run main.go -remote -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport

test-import-local: 
	go run main.go -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport

test-import-local-file: 
	go run main.go -inputFile ../data.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
