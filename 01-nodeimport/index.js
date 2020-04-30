#!/usr/bin/env node

import "path";
import yargs from "yargs";
import fastCsv from "fast-csv";
import * as path from "path";
import AWS from "aws-sdk";

const batchOf = (size = 25, execute) => {
  const items = [];
  return async (item, last = false) => {
    if (item) {
      items.push(item);
    }
    if (last || items.length === size) {
      await execute(items);
      items.length = 0;
    }
  };
};

const bools = {
  true: true,
  TRUE: true,
  false: false,
  FALSE: false,
};

const toDynamoItem = (o) => {
  const item = {};
  Object.keys(o).forEach((key) => {
    const value = o[key];
    if (bools[value] !== undefined) {
      item[key] = { BOOL: bools[value] };
      return;
    }
    if (!isNaN(value)) {
      item[key] = { N: value };
      return;
    }
    item[key] = { S: value };
  });
  return item;
};

const toPutRequest = (item) => ({ PutRequest: { Item: toDynamoItem(item) } });

const batchWrite = async ({ client, table, items }) => {
  const ri = {};
  ri[table] = items.map((itm) => toPutRequest(itm));
  const params = { RequestItems: ri };
  await client.batchWriteItem(params).promise();
};

const dynamoImport = async ({ region, table, csv, delimiter }) => {
  const fileName = path.resolve(csv);

  const start = process.hrtime()[0];
  const client = new AWS.DynamoDB({ region });
  let count = 0;
  const processor = batchOf(25, async (items) => {
    await batchWrite({ client, table, items });
    count += items.length;
    if (count % 100 == 0) {
      const seconds = (process.hrtime()[0] - start);
      const itemsPerSecond = count / seconds;
      console.log(
        `Inserted ${count} records in ${seconds}s - ${itemsPerSecond} records per second`
      );
    }
  });

  let rowIndex = 0;
  const parser = fastCsv
    .parseFile(fileName, { headers: true, delimiter })
    .on("error", (error) => console.error(error))
    .on("data", async (row) => {
      parser.pause();
      try {
        await processor(row);
        rowIndex++;
      } catch (err) {
        console.log(`Error processing row ${rowIndex}: ${err}`);
      } finally {
        parser.resume();
      }
    })
    .on("end", async (rowCount) => {
      await batch(null, true);
      console.log(`Parsed ${rowCount} rows`);
    });
};

const argv = yargs(process.argv)
  .usage(
    "Usage: $0 --region=eu-west-2 --table=ddbimport --csv=../data.csv --delimiter=comma"
  )
  .describe("region", "DynamoDB region.")
  .describe("table", "DynamoDB table.")
  .describe("csv", "Path to file to import.")
  .describe("delimiter", "tab / comma")
  .demandOption(["region", "table", "csv"])
  .help("h")
  .alias("h", "help").argv;

argv.delimiter = argv.delimiter === "tab" ? "\t" : ",";

dynamoImport(argv);
