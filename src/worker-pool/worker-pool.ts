
import sourceMapSupport from 'source-map-support';
sourceMapSupport.install();

import { Worker, isMainThread, parentPort } from 'worker_threads';
import os from 'os';

import { sleep } from '../lib/sleep';
import { CsvLogParseResult } from '../csv-parse/parse-csv-log';
import { CsvConvertResult } from '../csv-logs/convert-csv-path-date';
import { CsvPathDate } from '../lib/date-time-util';
import { CsvLogConvertResult } from '../csv-logs/convert-csv-log';
import { HashLogResult } from '../csv-logs/hash-log';
import { CsvReadResult } from '../csv-parse/read-csv-log';

import {
  MESSAGE_TYPES,
  ASYNC_READ_RECORD_QUEUE_SIZE,
} from './worker-constants';
import { WorkerMessage } from './worker-message';
import {
  handleAck, handleConvertCsv, handleConvertCsvLog, handleCsvRead, handleCsvWrite, handleCsvWriter, handleCsvWriterEnd, handleCsvWriterWrite, handleExit, handleHashFile, handleParseCsv,
} from './worker-handlers';

const NUM_CPUS = os.cpus().length;
export const NUM_WORKERS = Math.round(
  // 1
  // 2
  // NUM_CPUS - 2
  // NUM_CPUS - 1
  // NUM_CPUS
  NUM_CPUS / Math.LOG2E
  // NUM_CPUS / 2
  // NUM_CPUS / 4
  // NUM_CPUS * 2
  // NUM_CPUS * 4
  // NUM_CPUS * 8
  // NUM_CPUS - 2
);

const MAX_CSV_WRITERS = Math.floor(
  // 1
  // 2
  // 3
  // NUM_WORKERS / 2
  NUM_WORKERS / 1.5
);

let workers: Worker[] = [];
let availableWorkers: Worker[] = [];

type CsvWriterJobTuple = [
  string,
  (csvPath: string) => void, // resolver
  () => Worker, // running worker
];

let csvWriterQueue: CsvWriterJobTuple[] = [];
let runningCsvWriterJobs: CsvWriterJobTuple[] = [];

let hashQueue: [ string, (hashResult: HashLogResult) => void ][] = [];
let runningHashJobs: [ string, (hashResult: HashLogResult) => void ][] = [];

let parseQueue: [ string, (parseResult: CsvLogParseResult) => void ][] = [];
let runningParseJobs: [ string, (parseResult: CsvLogParseResult) => void ][] = [];

type ReadCsvJobTuple = [
  string,
  (readReadResult: CsvReadResult) => void,
  () => void,
  (records: any[]) => void,
];

let readCsvQueue: ReadCsvJobTuple[] = [];
let runningReadCsvJobs: ReadCsvJobTuple[] = [];

let convertQueue: [ CsvPathDate, (convertResult: CsvConvertResult) => void ][] = [];
let runningConvertJobs: [ CsvPathDate, (convertResult: CsvConvertResult) => void ][] = [];

let convertLogQueue: [
  string,
  (convertLogResult: CsvLogConvertResult) => void,
  (records: any[][]) => Promise<void>,
  () => void,
][] = [];
let runningConvertLogJobs: [
  string,
  (converLogResult: CsvLogConvertResult) => void,
  (records: any[][]) => Promise<void>,
  () => void,
][] = [];

let csvWriteQueue: [ string, any[][], (err?:any) => void ][] = [];
let runningCsvWriteJobs: [ string, any[][], (err?:any) => void ][] = [];

let isInitialized = false;
let areWorkersDestroyed = false;

export interface AsyncCsvWriter {
  write: (records: any[][]) => Promise<void>;
  end: () => Promise<void>;
}

if(!isMainThread) {
  initWorkerThread();
}

export async function getAsyncCsvWriter(filePath: string): Promise<AsyncCsvWriter> {
  let isWriting: boolean;
  let writerJobIdx: number, writerJob: CsvWriterJobTuple, writerWorker: Worker;

  await queueCsvWriterJob(filePath);
  writerJobIdx = runningCsvWriterJobs.findIndex(csvWriterJob => {
    return csvWriterJob[0] === filePath;
  });
  if(writerJobIdx === -1) {
    throw new Error(`Error finding job for ${filePath}`);
  }
  writerJob = runningCsvWriterJobs[writerJobIdx];
  writerWorker = writerJob[2]();

  if(writerWorker === undefined) {
    throw new Error(`undefined worker for job ${filePath}`);
  }
  await new Promise<void>(resolve => {
    let foundRunningJobIdx: number;
    writerWorker.once('message', msg => {
      if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER) {
        resolve();
      }
    })
  });

  isWriting = false;

  return {
    write,
    end,
  };

  async function write(records: any[][]) {
    while(isWriting) {
      await sleep(0);
    }

    isWriting = true;

    writerWorker.postMessage({
      messageType: MESSAGE_TYPES.CSV_WRITER_WRITE,
      data: {
        records,
      },
    });

    await new Promise<void>(resolve => {
      writerWorker.once('message', msg => {
        if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER_WRITE_DONE) {
          isWriting = false;
          resolve();
        }
      });
    });
  }

  async function end() {
    writerWorker.postMessage({
      messageType: MESSAGE_TYPES.CSV_WRITER_END,
    });
    return new Promise<void>(resolve => {
      writerWorker.once('message', msg => {
        let runningWriterIdx: number;
        
        if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER_END) {
          runningWriterIdx = runningCsvWriterJobs.findIndex(csvWriterJob => {
            return csvWriterJob[0] === filePath;
          });
          if(runningWriterIdx === -1) {
            throw new Error(`cannot find job to stop: ${filePath}`);
          }
          runningCsvWriterJobs.splice(runningWriterIdx, 1);

          addWorker(writerWorker);
          resolve();
        }
      });
    });
  }
}

function queueCsvWriterJob(targetCsvPath: string) {
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

export async function initializePool() {
  if(isMainThread) {
    if(!isInitialized) {
      await initMainThread();
    }
  }
}

export async function destroyWorkers() {
  let exitPromises: Promise<void>[];
  if(!isInitialized || areWorkersDestroyed) {
    return;
  }
  exitPromises = workers.map(worker => {
    return new Promise<void>((resolve, reject) => {
      worker.on('exit', err => {
        if(err) {
          return reject(err);
        }
        resolve();
      });
    });
  });
  workers.forEach(worker => {
    worker.postMessage({
      messageType: MESSAGE_TYPES.EXIT,
    });
  });
  await Promise.all(exitPromises);
  areWorkersDestroyed = true;
}

async function initMainThread() {
  let ackPromises: Promise<void>[];
  console.log('initializing main thread');
  console.log(`NUM_WORKERS: ${NUM_WORKERS}`);
  console.log(`ASYNC_READ_RECORD_QUEUE_SIZE: ${ASYNC_READ_RECORD_QUEUE_SIZE.toLocaleString()}`);
  console.log(`MAX_CSV_WRITERS: ${MAX_CSV_WRITERS}`)
  for(let i = 0; i < NUM_WORKERS; ++i) {
    let worker: Worker;
    worker = new Worker(__filename);
    workers.push(worker);
  }
  ackPromises = workers.map(worker => {
    return new Promise<void>((resolve, reject) => {
      worker.once('message', (msg: WorkerMessage) => {
        if(msg.messageType === MESSAGE_TYPES.ACK) {
          availableWorkers.push(worker);
          resolve();
        } else {
          reject(new Error(`Unexpected Message Type from Worker: ${msg.messageType}`));
        }
      });
    });
  });
  workers.forEach(worker => {
    worker.postMessage({
      messageType: MESSAGE_TYPES.ACK,
    });
  });
  await Promise.all(ackPromises);
  isInitialized = true;

  workers.forEach(worker => {
    worker.on('message', msg => {
      let messageType: MESSAGE_TYPES;
      let foundConvertLogJobIdx: number;
      let foundConvertLogJob: [
        string,
        (convertLogResult: CsvLogConvertResult) => void,
        (records: any[][]) => Promise<void>,
        () => void,
      ];
      let foundCsvReadLogJob: ReadCsvJobTuple;
      let foundCsvReadLogJobIdx: number;

      let foundCsvWriterJob: CsvWriterJobTuple;
      let foundCsvWriterJobIdx: number;

      messageType = msg?.messageType;
      if(messageType === undefined) {
        console.error('malformed message in main from worker:');
        console.error(msg);
      }

      switch(messageType) {
        case MESSAGE_TYPES.PARSE_CSV_COMPLETE:
          let foundParseJobIdx: number;
          let foundParseJob: [ string, (parseResult: CsvLogParseResult) => void ];
          foundParseJobIdx = runningParseJobs.findIndex((parseJob) => {
            return parseJob[0] === msg?.data?.csvPath;
          });
          if(foundParseJobIdx !== -1) {
            foundParseJob = runningParseJobs[foundParseJobIdx];
            runningParseJobs.splice(foundParseJobIdx, 1);
            addWorker(worker);
            foundParseJob[1](msg?.data?.parseResult);
          }
          break;
        case MESSAGE_TYPES.CONVERT_CSV_COMPLETE:
          let foundConvertJobIdx: number;
          let foundConvertJob: [ CsvPathDate, (convertResult: CsvConvertResult) => void ];
          foundConvertJobIdx = runningConvertJobs.findIndex((convertJob) => {
            return convertJob[0].dateStr === msg?.data?.csvPathDate?.dateStr;
          });
          if(foundConvertJobIdx !== -1) {
            foundConvertJob = runningConvertJobs[foundConvertJobIdx];
            runningConvertJobs.splice(foundConvertJobIdx, 1);
            addWorker(worker);
            foundConvertJob[1](msg?.data?.convertResult);
          }
          break;
        /*

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        v v v v v v v v v v v v v v v v v v v v v

        */
        case MESSAGE_TYPES.CSV_READ_COMPLETE:
          foundCsvReadLogJobIdx = runningReadCsvJobs.findIndex((readJob) => {
            return readJob[0] === msg?.data?.csvPath;
          });
          if(foundCsvReadLogJobIdx !== -1) {
            foundCsvReadLogJob = runningReadCsvJobs[foundCsvReadLogJobIdx];
            runningReadCsvJobs.splice(foundCsvReadLogJobIdx, 1);
            addWorker(worker);
            foundCsvReadLogJob[1](msg?.data?.readResult);
          }
          break;
        case MESSAGE_TYPES.ASYNC_CSV_READ:
          foundCsvReadLogJobIdx = runningReadCsvJobs.findIndex((readJob) => {
            return readJob[0] === msg?.data?.csvPath;
          });
          if(foundCsvReadLogJobIdx !== -1) {
            foundCsvReadLogJob = runningReadCsvJobs[foundCsvReadLogJobIdx];
            foundCsvReadLogJob[3](msg?.data?.records);
          }
          break;
        case MESSAGE_TYPES.CSV_WRITER:
          foundCsvWriterJobIdx = runningCsvWriterJobs.findIndex(writerJob => {
            return writerJob[0] === msg?.data?.csvPath;
          });
          if(foundCsvWriterJobIdx !== -1) {
            foundCsvWriterJob = runningCsvWriterJobs[foundCsvWriterJobIdx];
            foundCsvWriterJob[1](msg?.data?.csvPath);
          }
        
        /*

        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        v v v v v v v v v v v v v v v v v v v v v

        */
        case MESSAGE_TYPES.CONVERT_CSV_LOG_RECORD_READ:
          foundConvertLogJobIdx = runningConvertLogJobs.findIndex((convertLogJob) => {
            return convertLogJob[0] === msg?.data?.filePath;
          });
          if(foundConvertLogJobIdx !== -1) {
            foundConvertLogJob = runningConvertLogJobs[foundConvertLogJobIdx];
            foundConvertLogJob[2](msg?.data?.records);
          }
          break;
        case MESSAGE_TYPES.CONVERT_CSV_LOG_COMPLETE:
          foundConvertLogJobIdx = runningConvertLogJobs.findIndex((convertLogJob) => {
            return convertLogJob[0] === msg?.data?.filePath;
          });
          if(foundConvertLogJobIdx !== -1) {
            foundConvertLogJob = runningConvertLogJobs[foundConvertLogJobIdx];
            runningConvertLogJobs.splice(foundConvertLogJobIdx, 1);
            addWorker(worker);
            foundConvertLogJob[1](msg?.data?.convertLogResult);
          }
          break;
        case MESSAGE_TYPES.HASH_FILE_COMPLETE:
          let foundHashJobIdx: number;
          let foundHashJob: [ string, (hashResult: HashLogResult) => void ];
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
          break;
        case MESSAGE_TYPES.CSV_WRITE_COMPLETE:
          let foundCsvWriteJobIdx: number;
          let foundCsvWriteJob: [ string, any[][], () => void ];
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
    });
  });

  checkQueueLoop();
}

function checkQueueLoop() {
  setTimeout(() => {
    checkQueues(true);
    if(!areWorkersDestroyed) {
      checkQueueLoop();
    }
  }, 128);
}

function addWorker(worker: Worker) {
  availableWorkers.push(worker);
  checkQueues();
}

function removeWorker(): Worker {
  return availableWorkers.pop();
}

function checkQueues(queueLoop?: boolean) {
  let availableWorker: Worker, parseJob: [ string, (parseResult: CsvLogParseResult) => void ];
  let convertJob: [ CsvPathDate, (convertResult: CsvConvertResult) => void ];
  let convertLogJob: [
    string,
    (convertLogResult: CsvLogConvertResult) => void,
    (records: any[][]) => Promise<void>,
    () => void,
  ];
  let hashLogJob: [ string, (hashResult: HashLogResult) => void ];
  let csvWriteJob: [ string, any[][], (err?: any) => void];
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

function initWorkerThread() {
  parentPort.on('message', (msg: WorkerMessage) => {
    switch(msg.messageType) {
      case MESSAGE_TYPES.ACK:
        handleAck(msg);
        break;
      case MESSAGE_TYPES.PARSE_CSV:
        handleParseCsv(msg);
        break;
      case MESSAGE_TYPES.CONVERT_CSV:
        handleConvertCsv(msg);
        break;
      /*

      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      v v v v v v v v v v v v v v v v v v v v v

      */
      case MESSAGE_TYPES.CSV_READ:
        handleCsvRead(msg);
        break;
      case MESSAGE_TYPES.CSV_WRITER:
        handleCsvWriter(msg);
        break;
      /*

      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      v v v v v v v v v v v v v v v v v v v v v

      */
      case MESSAGE_TYPES.CSV_WRITER_END:
        handleCsvWriterEnd(msg);
        break
      case MESSAGE_TYPES.CSV_WRITER_WRITE:
        handleCsvWriterWrite(msg);
        break;
      case MESSAGE_TYPES.CONVERT_CSV_LOG:
        handleConvertCsvLog(msg);
        break;
      case MESSAGE_TYPES.HASH_FILE:
        handleHashFile(msg);
        break;
      case MESSAGE_TYPES.CSV_WRITE:
        handleCsvWrite(msg);
        break;
      case MESSAGE_TYPES.EXIT:
        handleExit(msg);
        break;
    }
  });
}
