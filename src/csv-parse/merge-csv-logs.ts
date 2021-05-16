
import path, { ParsedPath } from 'path';

import { getIntuitiveTimeFromMs } from '../print';
import { ReadAndParseCsvLogsResult, readCsvLogs } from './read-csv-logs';
import { listDir } from '../lib/files';
import { CONVERTED_CSV_LOGS_DIR_PATH } from '../constants';

let ASYNC_RECORDS_QUEUE_WINDOW: number;
let ASYNC_RECORDS_QUEUE_MAX: number;

let PROCESS_ASYNC_MOD: number;
let PROCESS_ASYNC_WAIT_MS: number;

let LOGS_IN_PAST: number;
let LOGS_TO_INCLUDE: number;

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

// LOGS_IN_PAST = 0;
// LOGS_IN_PAST = 30;
// LOGS_IN_PAST = 100;
// LOGS_IN_PAST = 140;
// LOGS_IN_PAST = 190;
// LOGS_IN_PAST = 200;
LOGS_IN_PAST = 230;
// LOGS_IN_PAST = 292;
// LOGS_IN_PAST = 300;

// LOGS_TO_INCLUDE = 4;
LOGS_TO_INCLUDE = 7;
// LOGS_TO_INCLUDE = 14;
// LOGS_TO_INCLUDE = 28;
// LOGS_TO_INCLUDE = 56;
// LOGS_TO_INCLUDE = 112;
// LOGS_TO_INCLUDE = 224;

export async function mergeCsvLogs() {
  let readAndParseResult: ReadAndParseCsvLogsResult;
  let deltaT: number, deltaLabel: string;
  let recordCount: number, headers: any[];
  let uriMap: Map<string, number>;
  let csvLogPaths: string[];

  csvLogPaths = await listDir(CONVERTED_CSV_LOGS_DIR_PATH);
  csvLogPaths = sortCsvLogPaths(csvLogPaths);

  // console.log(`startCsvLogsSlice: ${startCsvLogsSlice}\nendCsvLogsSlice: ${endCsvLogsSlice}`);
  csvLogPaths = filterCsvPathsByDate(csvLogPaths);

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

  readAndParseResult = await readCsvLogs(csvLogPaths, parseRecordCb);

  [ deltaT, deltaLabel ] = getIntuitiveTimeFromMs(readAndParseResult.deltaMs);

  if(headers !== undefined) {
    // process.stdout.write(`\nHeaders: [ ${headers.join(', ')} ]`);
  }
  // printUriMap(uriMap);

  process.stdout.write(`\nRead took ${deltaT.toFixed(2)} ${deltaLabel}`);
  process.stdout.write('\n');
  process.stdout.write('\n');
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
