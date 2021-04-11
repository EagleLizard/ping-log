
import path, { ParsedPath } from 'path';
import fs, { WriteStream } from 'fs';
import os from 'os';

import _chunk from 'lodash.chunk';

import { CONVERTED_CSV_LOGS_DIR_PATH, PERIOD_STAT_PATH } from '../constants';
import { scanLog } from '../lib/csv-read';
import { listDir } from '../lib/files';
import { hashSync } from '../lib/hasher';
import { getIntuitiveTimeFromMs, printProgress } from '../print';
import { CsvAggregator } from './csv-aggregator';
import { DB } from '../db/db';

const NUM_CPUS = os.cpus().length;

const CSV_CHUNK_SIZE = Math.round(
  // 1
  // NUM_CPUS - 1
  // NUM_CPUS * 4
  // NUM_CPUS * Math.E
  // NUM_CPUS * Math.LOG2E
  NUM_CPUS / Math.E
  // NUM_CPUS / Math.LOG2E
  // NUM_CPUS / 4
);

console.log(`NUM_CPUS: ${NUM_CPUS}`);
console.log(`CSV_CHUNK_SIZE: ${CSV_CHUNK_SIZE}`);

export async function parseCsvLogs() {
  let db: DB;
  let csvLogPaths: string[], aggregator: CsvAggregator;

  db = await DB.initialize();

  csvLogPaths = await listDir(CONVERTED_CSV_LOGS_DIR_PATH);

  csvLogPaths = sortCsvLogPaths(csvLogPaths);

  // csvLogPaths = csvLogPaths.slice(-3);
  csvLogPaths = csvLogPaths.filter(csvLogPath => {
    const _dates = [
      '03-15-2021',
      '03-16-2021',
      '03-17-2021',
      '03-18-2021',
      '03-19-2021',

      '03-23-2021',
      '03-24-2021',
      '03-25-2021',
      '03-26-2021',
      '03-29-2021',
      '03-30-2021',
      '03-31-2021',

      '04-01-2021',
      '04-02-2021',
      '04-05-2021',
      '04-06-2021',
      '04-07-2021',
      '04-08-2021',
    ];
    return _dates.some(_date => {
      return csvLogPath.includes(_date);
    });
  });
  console.log(csvLogPaths);
  aggregator = await analyzeCsvLogs(csvLogPaths);
  await writeStat(aggregator);
}

async function analyzeCsvLogs(csvLogPaths: string[]): Promise<CsvAggregator> {
  let startMs: number, endMs: number, deltaMs: number, deltaT: number, deltaLabel: string;
  let csvChunks: string[][];
  let totalFileCount: number, completeCount: number, recordCount: number;
  let headers: string[], csvAggregator: CsvAggregator;

  csvAggregator = new CsvAggregator();

  totalFileCount = csvLogPaths.length;
  completeCount = 0;
  recordCount = 0;
  csvChunks = _chunk(csvLogPaths, CSV_CHUNK_SIZE);

  startMs = Date.now();
  for(let i = 0, currChunk: string[]; currChunk = csvChunks[i], i < csvChunks.length; ++i) {
    let scanLogPromises: Promise<void>[];
    scanLogPromises = [];
    for(let k = 0, currCsvPath: string; currCsvPath = currChunk[k], k < currChunk.length; ++k) {
      let scanLogPromise: Promise<void>;
      scanLogPromise = scanLog(currCsvPath, (record, recordIdx) => {
        if((recordIdx === 0) && (headers === undefined)) {
          headers = record;
          console.log(headers);
        } else if(recordIdx !== 0) {
          csvAggregator.aggregate(headers, record);
          recordCount++;
        }
      }).then(() => {
        completeCount++;
        printProgress(completeCount, totalFileCount);
      });
      scanLogPromises.push(scanLogPromise);
    }
    await Promise.all(scanLogPromises);
  }
  endMs = Date.now();

  process.stdout.write('\n');

  deltaMs = endMs - startMs;
  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  console.log(`\n${recordCount.toLocaleString()} records`);
  console.log(`Parse took: ${deltaT.toFixed(2)} ${deltaLabel}`);
  return csvAggregator;
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

/*
sha256 - c6568d21501a22198b820250f56b2947c18927040b9451c47df4d7060fd11eca
  hex:
    6: 2,934,205
    7: 226,105
    8: 14,110
    8: 58,451
    10: 54
    11: 2
    12: 0
    13: 0
  base64:
    2: 7,849,428
    4: 2,934,205
    6: 884
    7: 10
    8: 0
    8: 0
    9: 0

sha1 - 0781ab453183734f0bb17f2bb507e51d0b24700b
  hex:
    6: 2,933,867
    7: 227,132
    8: 14,439
    10: 54
    11: 2
    12: 0
    12: 2
    13: 0
  base64:
    2: 7,849,428
    4: 2,933,867
    6: 864
    7: 8
    8: 0
    8: 2
    9: 0

md5 - a5aacc6a8c68faa26dfa55f955700757
  hex:
    6: 2,931,451
    7: 225,357
    8:
    10: 62
    11: 6
    12: 0
    12: 0
    13: 
  base64:
    2: 7,849,428
    4: 2,931,451
    6: 996
    7: 
    8: 0
    8: 2
    9: 0
*/
