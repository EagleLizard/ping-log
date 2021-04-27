
import path, { ParsedPath } from 'path';
import fs, { WriteStream } from 'fs';

// import _chunk from 'lodash.chunk';

import { CONVERTED_CSV_LOGS_DIR_PATH, PERIOD_STAT_PATH } from '../constants';

import { listDir } from '../lib/files';
import { CsvAggregator } from '../csv-logs/csv-aggregator';
import { DB } from '../db/db';
import { analyzeCsvLogs } from './analyze-csv-logs';
import { CsvLogParseResult, parseCsvLog } from './parse-csv-log';
import { destroyWorkers, initializePool, queueParseCsv } from './worker-pool';
import { getIntuitiveTimeFromMs, printProgress } from '../print';

export async function parseCsvLogs() {
  let db: DB;
  let csvLogPaths: string[], aggregator: CsvAggregator, csvParseResults: CsvLogParseResult[];
  let startMs: number, endMs: number, deltaMs: number, deltaT: number, deltaLabel: string;
  let recordCount: number;
  let doneCount: number;
  doneCount = 0;

  db = await DB.initialize();

  csvLogPaths = await listDir(CONVERTED_CSV_LOGS_DIR_PATH);

  csvLogPaths = sortCsvLogPaths(csvLogPaths);

  csvLogPaths = csvLogPaths.slice(-8);
  console.log(`num logs: ${csvLogPaths.length}`);
  // aggregator = new CsvAggregator();
  startMs = Date.now();

  await initializePool();
  const csvJobPromises = csvLogPaths.map(csvLogPath => {
    return queueParseCsv(csvLogPath).then((parseResult) => {
      doneCount++;
      printProgress(doneCount, csvLogPaths.length);
      return parseResult;
    });
  });
  csvParseResults = await Promise.all(csvJobPromises);

  // csvParseResults = await parseLogs(csvLogPaths);

  aggregator = CsvAggregator.merge(
    csvParseResults.map(csvParseResult => csvParseResult.aggregator)
  );

  endMs = Date.now();
  deltaMs = endMs - startMs;
  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  recordCount = csvParseResults.reduce((acc, curr) => {
    return acc + curr.numRecords;
  }, 0);
  console.log(`\n${recordCount.toLocaleString()} records`);
  console.log(`parseLog took: ${deltaT.toFixed(2)} ${deltaLabel}`);
  console.log('\n');
  await destroyWorkers();

  startMs = Date.now();
  await writeStat(aggregator);
  endMs = Date.now();
  deltaMs = endMs - startMs;
  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  console.log(`writeStat took: ${deltaT.toFixed(2)} ${deltaLabel}`);
  console.log('\n');

  // startMs = Date.now();
  // aggregator = await analyzeCsvLogs(csvLogPaths);
  // endMs = Date.now();
  // deltaMs = endMs - startMs;
  // [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  // console.log(`parseLog took: ${deltaT.toFixed(2)} ${deltaLabel}`);
  // await writeStat(aggregator);
}

async function parseLogs(csvLogPaths: string[]): Promise<CsvLogParseResult[]> {
  let csvLogParseResults: CsvLogParseResult[];
  csvLogParseResults = [];
  for(let i = 0, currPath: string; currPath = csvLogPaths[i], i < csvLogPaths.length; ++i) {
    let currCsvLogParseResult: CsvLogParseResult;
    currCsvLogParseResult = await parseCsvLog(currPath);
    csvLogParseResults.push(currCsvLogParseResult);
    printProgress(i + 1, csvLogPaths.length);
  }
  process.stdout.write('\n');
  return csvLogParseResults;
}

function writeStat(csvAggregator: CsvAggregator): Promise<void> {
  return new Promise((resolve, reject) => {
    let writeStream: WriteStream;
    writeStream = fs.createWriteStream(PERIOD_STAT_PATH);
    writeStream.on('close', () => {
      resolve();
    });
    writeStream.on('error', err => {
      reject(err);
    });
    const stats = [ ...csvAggregator.timeBuckets ];
    stats.sort((a, b) => {
      let aKey: string, bKey: string;
      aKey = a[0];
      bKey = b[0];
      if(aKey > bKey) {
        return -1;
      }
      if(aKey < bKey) {
        return 1;
      }
      return 0;
    });
    stats.forEach(stat => {
      const [ key, bucket ] = stat;
      let pingAvg: number, failPercent: number;
      pingAvg = bucket.pingSum / bucket.successCount;
      failPercent = (bucket.failCount / bucket.recordCount) * 100;
      writeStream.write(`${key}\n${bucket.recordCount}\n${pingAvg.toFixed(2)}\n${failPercent.toFixed(1)}%\n\n`);
    });
    writeStream.end();
  });
}

function sortCsvLogPaths(csvLogPaths: string[]): string[] {
  csvLogPaths = csvLogPaths.slice();
  csvLogPaths.sort((a, b) => {
    let parsedA: ParsedPath, parsedB: ParsedPath,
      splatA: string[], splatB: string[],
      nameA: string, nameB: string;
    parsedA = path.parse(a);
    parsedB = path.parse(b);
    splatA = parsedA.name.split('-');
    splatB = parsedB.name.split('-');
    nameA = [ splatA[2], splatA[0], splatA[1] ].join('-');
    nameB = [ splatB[2], splatB[0], splatB[1] ].join('-');
    if(nameA > nameB) {
      return 1;
    }
    if(nameA < nameB) {
      return -1;
    }
    return 0;
  });
  return csvLogPaths;
}
