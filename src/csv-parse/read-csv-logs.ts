
import path from 'path';

import { readCsvLog, CsvReadResult } from './read-csv-log';
import { printProgress, getIntuitiveTimeFromMs } from '../print';
import { Timer } from '../lib/timer';
import { initializePool, destroyWorkers, queueCsvReadJob } from '../worker-pool/worker-pool';
import { sleep } from '../lib/sleep';

const DONE_COUNT_START_MOD = 16;

export interface ReadAndParseCsvLogsResult {
  deltaMs: number;
}

export async function readCsvLogs(csvLogPaths: string[], parseRecordCb: (record: any[]) => void): Promise<ReadAndParseCsvLogsResult> {
  let csvReadResults: CsvReadResult[];
  let timer: Timer, deltaMs: number, readMs: number, totalRecordCount: number,
    readsPerSecond: number;
  let deltaT: number, deltaLabel: string;

  console.log(`Total paths: ${csvLogPaths.length}`);
  const logPathFileNames = csvLogPaths.map(logPath => path.parse(logPath).name);
  console.log(logPathFileNames.join(', '));

  timer = Timer.start();
  csvReadResults = await readCsvLogsConcurrent(csvLogPaths, parseRecordCb);
  deltaMs = timer.stop();

  readMs = deltaMs;

  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  totalRecordCount = csvReadResults.reduce((acc, curr) => {
    return acc + curr.recordCount;
  }, 0);
  readsPerSecond = Math.round(totalRecordCount / (readMs / 1000));

  process.stdout.write('\n');
  process.stdout.write(`\nRead ${totalRecordCount.toLocaleString()} records in ${deltaT.toFixed(2)} ${deltaLabel}`);
  console.log(`\nR: ${readsPerSecond.toLocaleString()} records/second`);

  await destroyWorkers();

  return {
    deltaMs,
  };
}

async function readCsvLogsConcurrent(csvLogPaths: string[], recordCb: (records: any[]) => void) {
  let csvReadLogPromises: Promise<void>[], csvReadResults: CsvReadResult[];
  let doneCount: number, totalCount: number;
  let staggerMs: number, prevStaggerMs: number, currStaggerMs: number;
  let totalRecordCount: number, calledHeadersCb: boolean;
  calledHeadersCb = false;
  totalRecordCount = 0;

  await initializePool();

  doneCount = 0;
  totalCount = csvLogPaths.length * DONE_COUNT_START_MOD;
  csvReadLogPromises = [];
  csvReadResults = [];
  for(let i = 0, currCsvPath: string; currCsvPath = csvLogPaths[i], i < csvLogPaths.length; ++i) {
    let readCsvLogJobPromise: Promise<void>, readStartCb: () => void, recordsCb: (records: any[]) => void;
    let currRecordCount: number;
    currRecordCount = 0;
    readStartCb = () => {
      doneCount++;
      printProgress(doneCount, totalCount);
    };
    recordsCb = (records) => {
      for(let k = 0; k < records.length; ++k) {
        if(!calledHeadersCb) {
          recordCb(records[k]);
          calledHeadersCb = true;
          // console.log(`Headers: [ ${records[k].join(', ')} ]`);
        } else {
          if(currRecordCount > 0) {
            recordCb(records[k]);
          }
          currRecordCount++;
        }
      }
    };
    if(i !== 0) {
      staggerMs = 32;
      await sleep(staggerMs);
    }
    readCsvLogJobPromise = queueCsvReadJob(currCsvPath, readStartCb, recordsCb).then(readResult => {
      totalRecordCount += currRecordCount;
      doneCount = doneCount + (DONE_COUNT_START_MOD - 1);
      printProgress(doneCount, totalCount);
      csvReadResults.push(readResult);
    });
    csvReadLogPromises.push(readCsvLogJobPromise);
  }
  await Promise.all(csvReadLogPromises);
  process.stdout.write(`\nRecords: ${totalRecordCount.toLocaleString()}`);
  return csvReadResults;
}
