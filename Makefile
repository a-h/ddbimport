.PHONY: 01-nodeimport 02-goimport 03-goimport-gr

create-table:
	aws dynamodb create-table \
	    --table-name ddbimport \
	    --attribute-definitions AttributeName=ngram,AttributeType=S AttributeName=year,AttributeType=N \
	    --key-schema AttributeName=ngram,KeyType=HASH AttributeName=year,KeyType=RANGE \
	    --billing-mode PAY_PER_REQUEST 

download:
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-0.csv.zip -o 0.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-1.csv.zip -o 1.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-2.csv.zip -o 2.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-3.csv.zip -o 3.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-4.csv.zip -o 4.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-5.csv.zip -o 5.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-6.csv.zip -o 6.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-7.csv.zip -o 7.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-8.csv.zip -o 8.csv.zip
	curl http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-1M-1gram-20090715-9.csv.zip -o 9.csv.zip

prepare-data:
	# Add the headers.
	echo "ngram	year	match_count	page_count	volume_count" > data.csv
	# Prepare the data.
	unzip 0.csv.zip
	cat googlebooks-eng-1M-1gram-20090715-0.csv >> data.csv
	rm googlebooks-eng-1M-1gram-20090715-0.csv

01-nodeimport:
	cd 01-nodeimport && ./index.js --region=eu-west-2 --table ddbimport --csv ../data.csv --delimiter=tab --numericFields=year --keepAlive=false

02-goimport:
	go run 02-goimport/main.go -region=eu-west-2 -table=ddbimport -csv=data.csv -delimiter=tab -numericFields=year

03-goimport-gr:
	go run 03-goimport-gr/main.go -region=eu-west-2 -table=ddbimport -csv=data.csv -delimiter=tab -numericFields=year -concurrency=8

04-ec2:
	GOOS=linux go build ../03-go-import-gr/main.go -o ddbimport && scp -i ddbimport.key -r ddbimport ec2-user@ec2-35-178-249-154.eu-west-2.compute.amazonaws.com:/home/ec2-user 
