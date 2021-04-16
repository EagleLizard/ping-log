
import os from 'os';

import _chunk from 'lodash.chunk';

import { scanLog } from '../lib/csv-read';
import { getIntuitiveTimeFromMs, printProgress } from '../print';
import { CsvAggregator } from '../csv-logs/csv-aggregator';

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

export async function analyzeCsvLogs(csvLogPaths: string[]): Promise<CsvAggregator> {
  let startMs: number, endMs: number, deltaMs: number, deltaT: number, deltaLabel: string;
  let csvChunks: string[][];
  let totalFileCount: number, completeCount: number, recordCount: number;
  let headers: string[], csvAggregator: CsvAggregator;

  csvAggregator = new CsvAggregator();

  totalFileCount = csvLogPaths.length;
  completeCount = 0;
  recordCount = 0;
  csvChunks = _chunk(csvLogPaths, CSV_CHUNK_SIZE);

  // startMs = Date.now();
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
  // endMs = Date.now();

  process.stdout.write('\n');

  // deltaMs = endMs - startMs;
  // [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  console.log(`\n${recordCount.toLocaleString()} records`);
  // console.log(`Parse took: ${deltaT.toFixed(2)} ${deltaLabel}`);
  return csvAggregator;
}
