
import { Worker } from 'worker_threads';

import { CsvLogParseResult } from '../csv-parse/parse-csv-log';
import { CsvConvertResult } from '../csv-logs/convert-csv-path-date';
import { CsvPathDate } from '../lib/date-time-util';
import { CsvLogConvertResult } from '../csv-logs/convert-csv-log';
import { HashLogResult } from '../csv-logs/hash-log';
import { CsvReadResult } from '../csv-parse/read-csv-log';
import {
  MESSAGE_TYPES,
  MAX_CSV_WRITERS,
  MAX_CSV_READERS,
} from './worker-constants';

export interface AsyncCsvWriter {
  write: (records: any[][]) => Promise<void>;
  end: () => Promise<void>;
}

export type CsvWriterJobTuple = [
  string,
  (csvPath: string) => void, // resolver
  () => Worker, // running worker
];

export type ReadCsvJobTuple = [
  string,
  (readReadResult: CsvReadResult) => void,
  () => void,
  (records: any[]) => void,
];

export type ConvertLogJobTuple = [
  string,
  (convertLogResult: CsvLogConvertResult) => void,
  (records: any[][]) => Promise<void>,
  () => void,
];

export type HashJobTuple = [
  string,
  (hashResult: HashLogResult) => void,
];

export type ParseJobTuple = [
  string,
  (parseResult: CsvLogParseResult) => void,
];

export type ConvertJobTuple = [
  CsvPathDate,
  (convertResult: CsvConvertResult) => void,
];

export type CsvWriteJobTuple = [
  string,
  any[][],
  (err?:any) => void,
];

export const workers: Worker[] = [];
const availableWorkers: Worker[] = [];

const csvWriterQueue: CsvWriterJobTuple[] = [];
const runningCsvWriterJobs: CsvWriterJobTuple[] = [];

const hashQueue: HashJobTuple[] = [];
const runningHashJobs: HashJobTuple[] = [];

const parseQueue: ParseJobTuple[] = [];
const runningParseJobs: ParseJobTuple[] = [];

const readCsvQueue: ReadCsvJobTuple[] = [];
const runningReadCsvJobs: ReadCsvJobTuple[] = [];

const convertQueue: ConvertJobTuple[] = [];
const runningConvertJobs: ConvertJobTuple[] = [];

const convertLogQueue: ConvertLogJobTuple[] = [];
const runningConvertLogJobs: ConvertLogJobTuple[] = [];

const csvWriteQueue: CsvWriteJobTuple[] = [];
const runningCsvWriteJobs: CsvWriteJobTuple[] = [];

export function handleParseCsvComplete(msg: any, worker: Worker) {
  let foundParseJobIdx: number;
  let foundParseJob: ParseJobTuple;
  foundParseJobIdx = runningParseJobs.findIndex((parseJob) => {
    return parseJob[0] === msg?.data?.csvPath;
  });
  if(foundParseJobIdx !== -1) {
    foundParseJob = runningParseJobs[foundParseJobIdx];
    runningParseJobs.splice(foundParseJobIdx, 1);
    addWorker(worker);
    foundParseJob[1](msg?.data?.parseResult);
  }
}

export function handleConverCsvComplete(msg: any, worker: Worker) {
  let foundConvertJobIdx: number;
  let foundConvertJob: ConvertJobTuple;
  foundConvertJobIdx = runningConvertJobs.findIndex((convertJob) => {
    return convertJob[0].dateStr === msg?.data?.csvPathDate?.dateStr;
  });
  if(foundConvertJobIdx !== -1) {
    foundConvertJob = runningConvertJobs[foundConvertJobIdx];
    runningConvertJobs.splice(foundConvertJobIdx, 1);
    addWorker(worker);
    foundConvertJob[1](msg?.data?.convertResult);
  }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
v v v v v v v v v v v v v v v v v v v v v

*/
export function handleCsvReadComplete(msg: any, worker: Worker) {
  let foundCsvReadLogJob: ReadCsvJobTuple;
  let foundCsvReadLogJobIdx: number;
  foundCsvReadLogJobIdx = runningReadCsvJobs.findIndex((readJob) => {
    return readJob[0] === msg?.data?.csvPath;
  });
  if(foundCsvReadLogJobIdx !== -1) {
    foundCsvReadLogJob = runningReadCsvJobs[foundCsvReadLogJobIdx];
    runningReadCsvJobs.splice(foundCsvReadLogJobIdx, 1);
    addWorker(worker);
    foundCsvReadLogJob[1](msg?.data?.readResult);
  }
}

export function handleAsyncCsvRead(msg: any) {
  let foundCsvReadLogJob: ReadCsvJobTuple;
  let foundCsvReadLogJobIdx: number;
  foundCsvReadLogJobIdx = runningReadCsvJobs.findIndex((readJob) => {
    return readJob[0] === msg?.data?.csvPath;
  });
  if(foundCsvReadLogJobIdx !== -1) {
    foundCsvReadLogJob = runningReadCsvJobs[foundCsvReadLogJobIdx];
    foundCsvReadLogJob[3](msg?.data?.records);
  }
}

export function handleCsvWriterMain(msg: any) {
  let foundCsvWriterJob: CsvWriterJobTuple;
  let foundCsvWriterJobIdx: number;
  foundCsvWriterJobIdx = runningCsvWriterJobs.findIndex(writerJob => {
    return writerJob[0] === msg?.data?.csvPath;
  });
  if(foundCsvWriterJobIdx !== -1) {
    foundCsvWriterJob = runningCsvWriterJobs[foundCsvWriterJobIdx];
    foundCsvWriterJob[1](msg?.data?.csvPath);
  }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
v v v v v v v v v v v v v v v v v v v v v

*/

export function handleConvertCsvLogRecordRead(msg: any) {
  let foundConvertLogJobIdx: number;
  let foundConvertLogJob: ConvertLogJobTuple;
  foundConvertLogJobIdx = runningConvertLogJobs.findIndex((convertLogJob) => {
    return convertLogJob[0] === msg?.data?.filePath;
  });
  if(foundConvertLogJobIdx !== -1) {
    foundConvertLogJob = runningConvertLogJobs[foundConvertLogJobIdx];
    foundConvertLogJob[2](msg?.data?.records);
  }
}

export function handleConvertCsvLogComplete(msg: any, worker: Worker) {
  let foundConvertLogJobIdx: number;
  let foundConvertLogJob: ConvertLogJobTuple;
  foundConvertLogJobIdx = runningConvertLogJobs.findIndex((convertLogJob) => {
    return convertLogJob[0] === msg?.data?.filePath;
  });
  if(foundConvertLogJobIdx !== -1) {
    foundConvertLogJob = runningConvertLogJobs[foundConvertLogJobIdx];
    runningConvertLogJobs.splice(foundConvertLogJobIdx, 1);
    addWorker(worker);
    foundConvertLogJob[1](msg?.data?.convertLogResult);
  }
}

export function handleHashFileComplete(msg: any, worker: Worker) {
  let foundHashJobIdx: number;
  let foundHashJob: HashJobTuple;
  foundHashJobIdx = runningHashJobs.findIndex((hashLogJob) => {
    return hashLogJob[0] === msg?.data?.filePath;
  });
  if(foundHashJobIdx !== -1) {
    foundHashJob = runningHashJobs[foundHashJobIdx];
    runningHashJobs.splice(foundHashJobIdx, 1);
    addWorker(worker);
    foundHashJob[1]({
      filePath: msg?.data?.filePath,
      fileHash: msg?.data?.fileHash,
    });
  }
}

export function handleCsvWriteComplete(msg: any, worker: Worker) {
  let foundCsvWriteJobIdx: number;
  let foundCsvWriteJob: CsvWriteJobTuple;
  foundCsvWriteJobIdx = runningCsvWriteJobs.findIndex(csvWriteJob => {
    return csvWriteJob[0] === msg?.data?.targetCsvPath;
  });
  if(foundCsvWriteJobIdx !== -1) {
    foundCsvWriteJob = runningCsvWriteJobs[foundCsvWriteJobIdx];
    runningCsvWriteJobs.splice(foundCsvWriteJobIdx, 1);
    addWorker(worker);
    foundCsvWriteJob[2]();
  }
}

/* ------------------------------- */

export function queueCsvWriterJob(targetCsvPath: string) {
  return new Promise<string>((resolve, reject) => {
    const resolver = (csvPath: string) => {
      resolve(csvPath);
    };
    csvWriterQueue.push([
      targetCsvPath,
      resolver,
      () => undefined,
    ]);
  });
}

export function queueHashJob(filePath: string) {
  return new Promise<HashLogResult>((resolve, reject) => {
    const resolver = (hashResult: HashLogResult) => {
      resolve(hashResult);
    };
    hashQueue.push([
      filePath,
      resolver,
    ]);
  });
}

export function queueParseCsv(csvPath: string) {
  return new Promise<CsvLogParseResult>((resolve, reject) => {
    const resolver = (parseResult: CsvLogParseResult) => {
      resolve(parseResult);
    };
    parseQueue.push([
      csvPath,
      resolver,
    ]);
  });
}
export function queueConvertCsv(csvPathDate: CsvPathDate) {
  return new Promise<CsvConvertResult>((resolve, reject) => {
    const resolver = (convertResult: CsvConvertResult) => {
      resolve(convertResult);
    };
    convertQueue.push([
      csvPathDate,
      resolver,
    ]);
  });
}
export function queueConvertCsvLog(csvPath: string, recordsCb: (records: any[][]) => Promise<void>, readStartCb: () => void) {
  return new Promise<CsvLogConvertResult>((resolve, reject) => {
    const resolver = (convertLogResult: CsvLogConvertResult) => {
      resolve(convertLogResult);
    };
    convertLogQueue.push([
      csvPath,
      resolver,
      recordsCb,
      readStartCb,
    ]);
  });
}
export function queueCsvReadJob(targetCsvPath: string, readStartCb: () => void, recordsCb: (records: any[]) => void) {
  return new Promise<CsvReadResult>((resolve, reject) => {
    const resolver = (csvReadResult: CsvReadResult) => {
      resolve(csvReadResult);
    };
    readCsvQueue.push([
      targetCsvPath,
      resolver,
      readStartCb,
      recordsCb,
    ]);
  });
}


export function getRunningWriterJob(filePath: string): CsvWriterJobTuple {
  let writerJobIdx: number, writerJob: CsvWriterJobTuple;
  writerJobIdx = runningCsvWriterJobs.findIndex(csvWriterJob => {
    return csvWriterJob[0] === filePath;
  });
  if(writerJobIdx === -1) {
    throw new Error(`Error finding job for ${filePath}`);
  }
  writerJob = runningCsvWriterJobs[writerJobIdx];
  return writerJob;
}

export function removeRunningCsvWriterJob(filePath: string, worker: Worker) {
  let runningWriterIdx: number;
  runningWriterIdx = runningCsvWriterJobs.findIndex(csvWriterJob => {
    return csvWriterJob[0] === filePath;
  });
  if(runningWriterIdx === -1) {
    throw new Error(`cannot find job to stop: ${filePath}`);
  }
  runningCsvWriterJobs.splice(runningWriterIdx, 1);
  addWorker(worker);
}

export function addWorker(worker: Worker) {
  availableWorkers.push(worker);
  checkQueues();
}

function removeWorker(): Worker {
  return availableWorkers.pop();
}

export function checkQueues() {
  let availableWorker: Worker;
  let parseJob: ParseJobTuple;
  let convertJob: ConvertJobTuple;
  // let convertLogJob: [
  //   string,
  //   (convertLogResult: CsvLogConvertResult) => void,
  //   (records: any[][]) => Promise<void>,
  //   () => void,
  // ];
  let convertLogJob: ConvertLogJobTuple;
  let hashLogJob: HashJobTuple;
  let csvWriteJob: CsvWriteJobTuple;
  let csvWriterJob: CsvWriterJobTuple;
  let csvReadJob: ReadCsvJobTuple;
  let readStartCb: () => void;

  if(availableWorkers.length > 0) {
    while((availableWorkers.length > 0) && (parseQueue.length > 0)) {
      availableWorker = removeWorker();
      parseJob = parseQueue.shift();
      runningParseJobs.push(parseJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.PARSE_CSV,
        data: {
          csvPath: parseJob[0],
        },
      });
    }
    while((availableWorkers.length > 0) && (convertQueue.length > 0)) {
      availableWorker = removeWorker();
      convertJob = convertQueue.shift();
      runningConvertJobs.push(convertJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CONVERT_CSV,
        data: {
          csvPathDate: convertJob[0],
        },
      });
    }
    while(
      (availableWorkers.length > 0)
      && (convertLogQueue.length > 0)
      && (runningConvertLogJobs.length < MAX_CSV_READERS)
    ) {
      availableWorker = removeWorker();
      convertLogJob = convertLogQueue.shift();
      runningConvertLogJobs.push(convertLogJob);
      convertLogJob[3](); // recordStartCb
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CONVERT_CSV_LOG,
        data: {
          filePath: convertLogJob[0],
        },
      });
    }
    while(
      (availableWorkers.length > 0)
      && (hashQueue.length > 0)
      // && (runningHashJobs.length < 3)
    ) {
      availableWorker = removeWorker();
      hashLogJob = hashQueue.shift();
      runningHashJobs.push(hashLogJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.HASH_FILE,
        data: {
          filePath: hashLogJob[0],
        },
      });
    }
    while((availableWorkers.length > 0) && (csvWriteQueue.length > 0)) {
      availableWorker = removeWorker();
      csvWriteJob = csvWriteQueue.shift();
      runningCsvWriteJobs.push(csvWriteJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CSV_WRITE,
        data: {
          targetCsvPath: csvWriteJob[0],
          rows: csvWriteJob[1],
        },
      });
    }
    while(
      (availableWorkers.length > 0)
        && (csvWriterQueue.length > 0)
        && (runningCsvWriterJobs.length < MAX_CSV_WRITERS)
      ) {
      availableWorker = removeWorker();
      csvWriterJob = csvWriterQueue.shift();
      csvWriterJob[2] = () => availableWorker;
      runningCsvWriterJobs.push(csvWriterJob);
      csvWriterJob[1](csvWriterJob[0]);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CSV_WRITER,
        data: {
          csvPath: csvWriterJob[0],
        },
      });
    }
    while((availableWorkers.length > 0) && (readCsvQueue.length > 0)) {
      availableWorker = removeWorker();
      csvReadJob = readCsvQueue.shift();
      readStartCb = csvReadJob[2];
      runningReadCsvJobs.push(csvReadJob);
      readStartCb();
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CSV_READ,
        data: {
          csvPath: csvReadJob[0],
        },
      });
    }
  }
}
