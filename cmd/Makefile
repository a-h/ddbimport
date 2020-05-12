test-import-remote: 
	./ddbimport_macos -remote -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport

test-import-local: 
	./ddbimport_macos -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport

test-import-local-file: 
	./ddbimport_macos -inputFile ../data.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
