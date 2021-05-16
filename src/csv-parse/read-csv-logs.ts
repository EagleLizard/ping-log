
import path, { ParsedPath } from 'path';

import { readCsvLog, CsvReadResult } from './read-csv-log';
import { listDir } from '../lib/files';
import { CONVERTED_CSV_LOGS_DIR_PATH } from '../constants';
import { printProgress, getIntuitiveTimeFromMs } from '../print';
import { Timer } from '../lib/timer';
import { initializePool, destroyWorkers, queueCsvReadJob, NUM_WORKERS } from './worker-pool';
import { sleep } from '../lib/sleep';

const DONE_COUNT_START_MOD = 16;

const RECORD_QUEUE: any[][] = [];
const ASYNC_RECORDS_QUEUE: any[][] =  [];
let ASYNC_RECORDS_QUEUE_WINDOW: number;
let ASYNC_RECORDS_QUEUE_MAX: number;
let RECORD_QUEUE_MAX: number;
let PROCESS_ASYNC_MOD: number;
let PROCESS_ASYNC_WAIT_MS: number;

RECORD_QUEUE_MAX = 1.024e4;
// RECORD_QUEUE_MAX = 512;

// ASYNC_RECORDS_QUEUE_MAX = 32e2;
// ASYNC_RECORDS_QUEUE_MAX = 64e2;
ASYNC_RECORDS_QUEUE_MAX = 128e2;
// ASYNC_RECORDS_QUEUE_MAX = 256e2;
// ASYNC_RECORDS_QUEUE_MAX = 512e2;
// ASYNC_RECORDS_QUEUE_MAX = 1024e3;
// ASYNC_RECORDS_QUEUE_MAX = 2048e3;
// ASYNC_RECORDS_QUEUE_MAX = 0.4096e6;
// ASYNC_RECORDS_QUEUE_MAX = 0.25e6;

// ASYNC_RECORDS_QUEUE_WINDOW = ASYNC_RECORDS_QUEUE_MAX;
// ASYNC_RECORDS_QUEUE_WINDOW = Math.round(ASYNC_RECORDS_QUEUE_MAX / Math.LOG2E);
// ASYNC_RECORDS_QUEUE_WINDOW = Math.round(ASYNC_RECORDS_QUEUE_MAX * Math.LOG10E);
ASYNC_RECORDS_QUEUE_WINDOW = Math.round(ASYNC_RECORDS_QUEUE_MAX / Math.E);

// PROCESS_ASYNC_MOD = 150;
// PROCESS_ASYNC_MOD = 100;
// PROCESS_ASYNC_MOD = 50;
// PROCESS_ASYNC_MOD = 25;
PROCESS_ASYNC_MOD = 5;

export interface ReadAndParseCsvLogsResult {
  deltaMs: number;
}

export async function readCsvLogs(csvLogPaths: string[], parseRecordCb: (record: any[]) => void): Promise<ReadAndParseCsvLogsResult> {
  let csvReadResults: CsvReadResult[];
  let timer: Timer, deltaMs: number, readSumMs: number, totalRecordCount: number,
    readsPerSecond: number;
  let deltaT: number, deltaLabel: string;
  let stopCheckingAsyncRecords: boolean, processRecordsPrintTimer: number, queueClears: number, processAsyncCallCount: number;
  let recordCb: (record: any[]) => void;

  console.log(`Total paths: ${csvLogPaths.length}`);
  const logPathFileNames = csvLogPaths.map(logPath => path.parse(logPath).name);
  console.log(logPathFileNames.join(', '));

  stopCheckingAsyncRecords = false;

  const clearRecordQueue = () => {
    // ASYNC_RECORDS_QUEUE.push(...RECORD_QUEUE);
    for(let i = 0, record: any[]; record = RECORD_QUEUE[i], i < RECORD_QUEUE.length; ++i) {
      ASYNC_RECORDS_QUEUE.push(record);
    }
    RECORD_QUEUE.length = 0;
  };
  recordCb = (record) => {
    RECORD_QUEUE.push(record);
    if(RECORD_QUEUE.length >= RECORD_QUEUE_MAX) {
      clearRecordQueue();
    }
  };

  processRecordsAsync(parseRecordCb);
  timer = Timer.start();
  // csvReadResults = await readCsvLogsSync(csvLogPaths, recordCb);
  // csvReadResults = await readCsvLogsAsync(csvLogPaths);
  csvReadResults = await readCsvLogsConcurrent(csvLogPaths, recordCb);
  if(RECORD_QUEUE.length > 0) {
    clearRecordQueue();
  }
  stopCheckingAsyncRecords = true;
  while(ASYNC_RECORDS_QUEUE.length > 0) {
    await sleep(5);
  }
  deltaMs = timer.stop();

  readSumMs = deltaMs;

  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(deltaMs);
  totalRecordCount = csvReadResults.reduce((acc, curr) => {
    return acc + curr.recordCount;
  }, 0);
  readsPerSecond = Math.round(totalRecordCount / (readSumMs / 1000));

  process.stdout.write('\n');
  process.stdout.write(`\nRead ${totalRecordCount.toLocaleString()} records in ${deltaT.toFixed(2)} ${deltaLabel}`);
  console.log(`\nR: ${readsPerSecond.toLocaleString()} records/second`);

  await destroyWorkers();

  return {
    deltaMs,
  };

  function processRecordsAsync(_parseRecordCb: (record: any[]) => void) {
    if(queueClears === undefined) {
      queueClears = 0;
    }
    if(processAsyncCallCount === undefined) {
      processAsyncCallCount = 0;
    }
    if(processRecordsPrintTimer === undefined) {
      processRecordsPrintTimer = Date.now();
    }
    setImmediate(() => {
      let recordsToProcess: any[][];
      if((stopCheckingAsyncRecords === true) && ASYNC_RECORDS_QUEUE.length < 1) {
        console.log(`\nqueueClears: ${queueClears}`);
        return;
      }
      processAsyncCallCount++;
      if((Date.now() - processRecordsPrintTimer) > 1.5e3) {
        processRecordsPrintTimer = Date.now();
      }
      if(ASYNC_RECORDS_QUEUE.length > ASYNC_RECORDS_QUEUE_MAX) {
        queueClears++;
        recordsToProcess = ASYNC_RECORDS_QUEUE.splice(0);
      } else {
        recordsToProcess = ASYNC_RECORDS_QUEUE.splice(0, ASYNC_RECORDS_QUEUE_WINDOW);
      }
      for(let i = 0, currRecord: any[][]; currRecord = recordsToProcess[i], i < recordsToProcess.length; ++i) {
        _parseRecordCb(currRecord);
      }

      queueMicrotask(() => {
        if((processAsyncCallCount % PROCESS_ASYNC_MOD) === 0) {
          setImmediate(() => {
            processRecordsAsync(_parseRecordCb);
          });
          // setTimeout(() => {
          //   processRecordsAsync(_parseRecordCb);
          // }, PROCESS_ASYNC_WAIT_MS);
        } else {
          processRecordsAsync(_parseRecordCb);
        }
      });

    });
  }
}

// async function readAndParseCsvLogs(parseRecordCb: (record: any[]) => void): Promise<ReadAndParseCsvLogsResult> {
  
// }

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
  };
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
