
import path, { ParsedPath } from 'path';

import { readCsvLog, CsvReadResult } from './read-csv-log';
import { listDir } from '../lib/files';
import { CONVERTED_CSV_LOGS_DIR_PATH } from '../constants';
import { printProgress, getIntuitiveTimeFromMs } from '../print';
import { Timer } from '../lib/timer';
import { initializePool, destroyWorkers, queueCsvReadJob, NUM_WORKERS } from './worker-pool';
import { sleep } from '../lib/sleep';

const DONE_COUNT_START_MOD = 16;

export async function readCsvLogs() {
  let csvLogPaths: string[], csvReadResults: CsvReadResult[];
  let timer: Timer, deltaMs: number, readSumMs: number, totalRecordCount: number,
    readsPerSecond: number;
  let deltaT: number, deltaLabel: string;
  let recordCount: number, headers: any[];
  let recordCb: (record: any[]) => void;

  csvLogPaths = await listDir(CONVERTED_CSV_LOGS_DIR_PATH);
  csvLogPaths = sortCsvLogPaths(csvLogPaths);

  csvLogPaths = csvLogPaths.slice(-7);
  // csvLogPaths = csvLogPaths.slice(-1 * (NUM_WORKERS + 1));

  console.log(`Total paths: ${csvLogPaths.length}`);
  const logPathFileNames = csvLogPaths.map(logPath => path.parse(logPath).name);
  console.log(logPathFileNames.join(', '));

  recordCount = 0;
  recordCb = (record) => {
    if(recordCount === 0) {
      headers = record;
    } else {
      if(isValidRecord(record)) {
        record = [
          +record[0],
          record[1],
          +record[2]
        ];
      } else {
        throw new Error(`Unexpected record: [ ${record.join(', ')} ]`);
      }
    }
    recordCount++;
  };
  timer = Timer.start();
  // csvReadResults = await readCsvLogsSync(csvLogPaths, recordCb);
  // csvReadResults = await readCsvLogsAsync(csvLogPaths);
  csvReadResults = await readCsvLogsConcurrent(csvLogPaths, recordCb);
  deltaMs = timer.stop();

  readSumMs = deltaMs;

  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  totalRecordCount = csvReadResults.reduce((acc, curr) => {
    return acc + curr.recordCount;
  }, 0);
  readsPerSecond = Math.round(totalRecordCount / (readSumMs / 1000));

  process.stdout.write('\n');
  if(headers !== undefined) {
    process.stdout.write(`\nHeaders: [ ${headers.join(', ')} ]`);
  }
  process.stdout.write(`\nRead ${recordCount.toLocaleString()} records in ${deltaT.toFixed(2)} ${deltaLabel}`);
  process.stdout.write(`\nRead ${totalRecordCount.toLocaleString()} records in ${deltaT.toFixed(2)} ${deltaLabel}`);
  console.log(`\nR: ${readsPerSecond.toLocaleString()} records/second`);
  process.stdout.write('\n');

  await destroyWorkers();
}

function isValidRecord(record: any[]) {
  return (
    ((typeof record[0]) === 'string')
    && ((typeof record[2]) === 'string')
  )
  && (
    /[0-9]+/ig.test(record[0])
    && /[0-9]+\.[0-9]{1,3}|NaN|[0-9]+/ig.test(record[2])
  );
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

async function readCsvLogsSync(csvLogPaths: string[], recordCb: (record: any[]) => void) {
  let csvReadResults: CsvReadResult[];
  let doneCount: number, totalCount: number, currRecordCount: number;
  doneCount = 0;
  totalCount = csvLogPaths.length;
  csvReadResults = [];
  const _recordCb = (record: any[], idx: any) => {
    if(idx !== 0) {
      recordCb(record);
    }
    // currRecordCount++;
  }
  for(let i = 0, currCsvPath: string; currCsvPath = csvLogPaths[i], i < csvLogPaths.length; ++i) {
    await readCsvLog(currCsvPath, _recordCb).then(readResult => {
      doneCount++;
      printProgress(doneCount, totalCount);
      csvReadResults.push(readResult);
    });
  }
  return csvReadResults;
}

async function readCsvLogsAsync(csvLogPaths: string[]) {
  let csvReadResults: CsvReadResult[], csvReadPromises: Promise<void>[];
  let doneCount: number, totalCount: number;
  doneCount = 0;
  totalCount = csvLogPaths.length;
  csvReadResults = [];
  csvReadPromises = [];
  for(let i = 0, currCsvPath: string; currCsvPath = csvLogPaths[i], i < csvLogPaths.length; ++i) {
    let csvReadPromise: Promise<void>;
    csvReadPromise = readCsvLog(currCsvPath).then(readResult => {
      doneCount++;
      printProgress(doneCount, totalCount);
      csvReadResults.push(readResult);
    });
    csvReadPromises.push(csvReadPromise);
  }
  await Promise.all(csvReadPromises);
  return csvReadResults;
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
