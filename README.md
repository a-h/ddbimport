## ddbimport

Import CSV data into DynamoDB.

## Features

* Comma separated (CSV) files
* Tab separated (TSV) files
* Large file sizes
* Local files
* Files on S3
* Parallel imports using AWS Step Functions to import > 4M rows per minute
* No depdendencies (no need for .NET, Python, Node.js, Docker, AWS CLI etc.)

## Warning

This program will use up all available DynamoDB capacity. It is not designed for use against production tables. Use at your own risk.

## Installation

Download binaries for MacOS, Linux and Windows at https://github.com/a-h/ddbimport/releases

A Docker image is available:

```
docker pull adrianhesketh/ddbimport
```

## Usage

### Import local CSV from local computer:

```
ddbimport -inputFile ../data.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
```

### Import S3 file from local computer:

```
ddbimport -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
```

### Import S3 file using remote ddbimport Step Function

```
ddbimport -remote -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport
```

### Install ddbimport Step Function

```
ddbimport -install -stepFnRegion=eu-west-2
```

## Benchmarks

Inserts per second of the Google ngram 1 dataset (English).

<img src="benchmarks.png"/>

