
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

let LOGS_IN_PAST: number;
let LOGS_TO_INCLUDE: number;

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

PROCESS_ASYNC_WAIT_MS = Math.round(PROCESS_ASYNC_MOD / 5);

LOGS_IN_PAST = 0;
// LOGS_IN_PAST = 30;
// LOGS_IN_PAST = 100;
// LOGS_IN_PAST = 140;
// LOGS_IN_PAST = 190;
// LOGS_IN_PAST = 200;
// LOGS_IN_PAST = 230;
// LOGS_IN_PAST = 292;
// LOGS_IN_PAST = 300;

LOGS_TO_INCLUDE = 4;
// LOGS_TO_INCLUDE = 7;
// LOGS_TO_INCLUDE = 14;
// LOGS_TO_INCLUDE = 28;
// LOGS_TO_INCLUDE = 56;
// LOGS_TO_INCLUDE = 112;
// LOGS_TO_INCLUDE = 224;

interface ReadAndParseCsvLogsResult {
  deltaMs: number;
}

export async function readCsvLogs() {
  let readAndParseResult: ReadAndParseCsvLogsResult;
  let deltaT: number, deltaLabel: string;
  let recordCount: number, headers: any[];
  let uriMap: Map<string, number>;

  console.log(`ASYNC_RECORDS_QUEUE_MAX: ${ASYNC_RECORDS_QUEUE_MAX}`);
  console.log(`ASYNC_RECORDS_QUEUE_WINDOW: ${ASYNC_RECORDS_QUEUE_WINDOW}`);
  console.log(`PROCESS_ASYNC_WAIT_MS: ${PROCESS_ASYNC_WAIT_MS}\nPROCESS_ASYNC_MOD: ${PROCESS_ASYNC_MOD}`);
  process.stdout.write('\n');

  recordCount = 0;
  uriMap = new Map;

  const parseRecordCb = (record: any[]) => {
    let uri: string;
    if(recordCount === 0) {
      headers = record;
    } else {
      if(isValidRecord(record)) {
        // record = [
        //   +record[0],
        //   record[1],
        //   +record[2]
        // ];
        uri = record[1];
        if(!uriMap.has(uri)) {
          uriMap.set(uri, 0);
        }
        uriMap.set(uri, uriMap.get(uri) + 1);
      } else {
        throw new Error(`Unexpected record: [ ${record.join(', ')} ]`);
      }
    }
    recordCount++;
  };

  readAndParseResult = await readAndParseCsvLogs(parseRecordCb);

  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(readAndParseResult.deltaMs);

  if(headers !== undefined) {
    // process.stdout.write(`\nHeaders: [ ${headers.join(', ')} ]`);
  }
  // printUriMap(uriMap);

  process.stdout.write(`\nRead took ${deltaT.toFixed(2)} ${deltaLabel}`);
  process.stdout.write('\n');
  process.stdout.write('\n');
}

async function readAndParseCsvLogs(parseRecordCb: (record: any[]) => void): Promise<ReadAndParseCsvLogsResult> {
  let csvLogPaths: string[], csvReadResults: CsvReadResult[];
  let timer: Timer, deltaMs: number, readSumMs: number, totalRecordCount: number,
    readsPerSecond: number;
  let deltaT: number, deltaLabel: string;
  let stopCheckingAsyncRecords: boolean, processRecordsPrintTimer: number, queueClears: number, processAsyncCallCount: number;
  let recordCb: (record: any[]) => void;

  csvLogPaths = await listDir(CONVERTED_CSV_LOGS_DIR_PATH);
  csvLogPaths = sortCsvLogPaths(csvLogPaths);

  // console.log(`startCsvLogsSlice: ${startCsvLogsSlice}\nendCsvLogsSlice: ${endCsvLogsSlice}`);
  csvLogPaths = filterCsvPathsByDate(csvLogPaths);

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

function filterCsvPathsByDate(csvLogPaths: string[]) {
  let today: Date, pastDateStart: Date, pastDateEnd: Date;
  let logPathDateTuples: [ string, Date ][], filteredLogPaths: string[];
  today = new Date();
  today = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  pastDateStart = new Date(today.getFullYear(), today.getMonth(), today.getDate() - LOGS_IN_PAST);
  pastDateEnd = new Date(today.getFullYear(), today.getMonth(), (today.getDate() - LOGS_IN_PAST) - LOGS_TO_INCLUDE);
  // if(LOGS_IN_PAST < LOGS_TO_INCLUDE) {
  //   LOGS_IN_PAST = LOGS_TO_INCLUDE + 1;
  // }
  // console.log('today');
  // console.log(today);
  // console.log('pastDateEnd');
  // console.log(pastDateEnd);
  // console.log('pastDateStart');
  // console.log(pastDateStart);
  logPathDateTuples = csvLogPaths.map(logPath => {
    let parsedPath: ParsedPath, logName: string;
    let splatDate: number[], day: number, month: number, year: number;
    let logDate: Date;
    parsedPath = path.parse(logPath);
    logName = parsedPath.name;
    splatDate = logName.split('-').map(val => +val);
    [ month, day, year ] = splatDate;
    logDate = new Date(year, month - 1, day);
    return [
      logPath,
      logDate,
    ];
  });
  logPathDateTuples = logPathDateTuples.filter(logDateTuple => {
    let logDate: Date;
    logDate = logDateTuple[1];
    return (logDate >= pastDateEnd)
      && (logDate < pastDateStart)
    ;
  });
  filteredLogPaths = logPathDateTuples.map(logDateTuple => logDateTuple[0]);
  return filteredLogPaths;
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

function printUriMap(uriMap: Map<string, number>) {
  let uriTuples: [ string, number ][];
  process.stdout.write('\n');
  uriTuples = [ ...uriMap ];
  uriTuples.sort((a, b) => {
    let aNum: number, bNum: number;
    aNum = a[1];
    bNum = b[1];
    if(aNum < bNum) {
      return 1;
    } else if(aNum > bNum) {
      return -1;
    }
    return 0;
  });
  const maxLen = uriTuples.reduce((acc, curr) => {
    if(curr[0].length > acc) {
      return curr[0].length;
    }
    return acc;
  }, -1);
  uriTuples = uriTuples.map(uriTuple => {
    uriTuple = [ uriTuple[0], uriTuple[1] ];
    uriTuple[0] = uriTuple[0].padEnd(maxLen, ' ');
    return uriTuple;
  });
  uriTuples.forEach(uriTuple => console.log(`${uriTuple[0]}: ${uriTuple[1].toLocaleString()}`));
}
