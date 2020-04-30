#!/usr/bin/env node

import "path";
import https from "https";
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

const stringItem = (s) => ({ S: s });
const numericItem = (s) => ({ N: s });
const booleanItem = (s) => {
  const bv = bools[value];
  if (bv === undefined) {
    return { BOOL: false };
  }
  return bv;
};

const toDynamoItems = (converters, items) =>
  items.map((item) => toDynamoItem(converters, item));

const toDynamoItem = (converters, o) => {
  const item = {};
  Object.keys(o).forEach((key) => {
    const value = o[key];
    const converter = converters[key];
    item[key] = converter === undefined ? stringItem(value) : converter(value);
  });
  return item;
};

const toPutRequest = (dynamoItem) => ({ PutRequest: { Item: dynamoItem } });

const batchWrite = async ({ client, table, dynamoItems }) => {
  const ri = {};
  ri[table] = dynamoItems.map((di) => toPutRequest(di));
  const params = { RequestItems: ri };
  await client.batchWriteItem(params).promise();
};

const since = (secs) => {
  const [nowS, nowNS] = process.hrtime();
  const now = nowS + nowNS / 1000000000;
  return now - secs;
};

const dynamoImport = async ({
  region,
  table,
  csv,
  delimiter,
  keepAlive,
  converters,
}) => {
  const fileName = path.resolve(csv);

  const [startS, startNS] = process.hrtime();
  const start = startS + startNS / 1000000000;

  // Using the keep-alive increases the throughput.
  const agent = new https.Agent({
    keepAlive,
  });
  const client = new AWS.DynamoDB({
    region,
    httpOptions: {
      agent,
    },
  });
  let count = 0;
  const processor = batchOf(25, async (items) => {
    const dynamoItems = toDynamoItems(converters, items);
    await batchWrite({ client, table, dynamoItems });
    count += items.length;
    if (count % 2500 == 0) {
      const seconds = since(start);
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
    "Usage: $0 --region=eu-west-2 --table=ddbimport --csv=../data.csv --delimiter=comma --keep-alive=true"
  )
  .describe("region", "DynamoDB region.")
  .describe("table", "DynamoDB table.")
  .describe("csv", "Path to file to import.")
  .describe(
    "keepAlive",
    "Whether to keep connections alive (true/false - default true)"
  )
  .describe("delimiter", "tab / comma")
  .describe(
    "numericFields",
    "Comma-separated list of fields that contain numeric values."
  )
  .describe(
    "booleanFields",
    "Comma-separated list of fields that contain boolean values."
  )
  .demandOption(["region", "table", "csv"])
  .help("h")
  .alias("h", "help").argv;

argv.delimiter = argv.delimiter === "tab" ? "\t" : ",";
argv.keepAlive = argv.keepAlive === "false" ? false : true;
argv.converters = {};
const addValueToKeys = (target, keySource, value) => {
  const keys = typeof(keySource) === "string" ? keySource.split(",") : [];
  keys.forEach((k) => (target[k] = value));
};
addValueToKeys(argv.converters, argv.numericFields, numericItem);
addValueToKeys(argv.converters, argv.booleanFields, booleanItem);

dynamoImport(argv);
