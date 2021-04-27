
import sourceMapSupport from 'source-map-support';
sourceMapSupport.install();

import { Worker, isMainThread, parentPort } from 'worker_threads';
import os from 'os';

import { sleep } from '../lib/sleep';
import { CsvLogParseResult, parseCsvLog } from './parse-csv-log';
import { convertCsvPathDate, CsvConvertResult } from '../csv-logs/convert-csv-path-date';
import { CsvPathDate } from '../lib/date-time-util';
import { CsvLogConvertResult, convertCsvLogFile } from '../csv-logs/convert-csv-log';
import { getFileHash, HashLogResult } from '../csv-logs/hash-log';
import { writeCsv } from '../lib/csv-writer';

const NUM_CPUS = os.cpus().length;
const NUM_WORKERS = Math.round(
  // 1
  // NUM_CPUS - 1
  NUM_CPUS / Math.LOG2E
  // NUM_CPUS / Math.E
  // NUM_CPUS / 2
  // NUM_CPUS / 4
  // NUM_CPUS
  // NUM_CPUS * 2
  // NUM_CPUS * 4
  // NUM_CPUS * 8
  // NUM_CPUS - 2
);

enum MESSAGE_TYPES {
  ACK = 'ack',
  EXIT = 'exit',
  HASH_FILE = 'hash_file',
  HASH_FILE_COMPLETE = 'hash_file_complete',
  PARSE_CSV = 'parse_csv',
  PARSE_CSV_COMPLETE = 'parse_csv_complete',
  CONVERT_CSV = 'convert_csv',
  CONVERT_CSV_COMPLETE = 'convert_csv_complete',
  CONVERT_CSV_LOG = 'convert_csv_log',
  CONVERT_CSV_LOG_COMPLETE = 'convert_csv_log_complete',
  CSV_WRITE = 'csv_write',
  CSV_WRITE_COMPLETE = 'csv_write_complete'
}

let workers: Worker[] = [];
let availableWorkers: Worker[] = [];

let hashQueue: [ string, (hashResult: HashLogResult) => void ][] = [];
let runningHashJobs: [ string, (hashResult: HashLogResult) => void ][] = [];

let parseQueue: [ string, (parseResult: CsvLogParseResult) => void ][] = [];
let runningParseJobs: [ string, (parseResult: CsvLogParseResult) => void ][] = [];

let convertQueue: [ CsvPathDate, (convertResult: CsvConvertResult) => void ][] = [];
let runningConvertJobs: [ CsvPathDate, (convertResult: CsvConvertResult) => void ][] = [];

let convertLogQueue: [ string, (convertLogResult: CsvLogConvertResult) => void ][] = [];
let runningConvertLogJobs: [ string, (converLogResult: CsvLogConvertResult) => void ][] = [];

let csvWriteQueue: [ string, any[][], (err?:any) => void ][] = [];
let runningCsvWriteJobs: [ string, any[][], (err?:any) => void ][] = [];

let isInitialized = false;
let areWorkersDestroyed = false;

interface WorkerMessage {
  messageType: MESSAGE_TYPES,
  data?: any,
}

if(!isMainThread) {
  initWorkerThread();
}

// (async () => {
//   try {
//     // await initializePool();
//   } catch(e) {
//     console.error(e);
//     throw e;
//   }
// })();

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
export function queueConvertCsvLog(csvPath: string) {
  return new Promise<CsvLogConvertResult>((resolve, reject) => {
    const resolver = (convertLogResult: CsvLogConvertResult) => {
      resolve(convertLogResult);
    };
    convertLogQueue.push([
      csvPath,
      resolver,
    ]);
  });
}
export function queueCsvWriteJob(targetCsvPath: string, rows: any[][]) {
  return new Promise<any>((resolve, reject) => {
    const resolver = (err?:any) => {
      resolve(err);
    };
    csvWriteQueue.push([
      targetCsvPath,
      rows,
      resolver,
    ]);
  });
}

export async function initializePool() {
  if(isMainThread) {
    if(!isInitialized) {
      await initMainThread();
    }
  } else {
    // initWorkerThread();
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
        case MESSAGE_TYPES.CONVERT_CSV_LOG_COMPLETE:
          let foundConvertLogJobIdx: number;
          let foundConvertLogJob: [ string, (convertLogResult: CsvLogConvertResult) => void ];
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
    checkQueues();
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

function checkQueues() {
  let availableWorker: Worker, parseJob: [ string, (parseResult: CsvLogParseResult) => void ];
  let convertJob: [ CsvPathDate, (convertResult: CsvConvertResult) => void ];
  let convertLogJob: [ string, (convertLogResult: CsvLogConvertResult) => void ];
  let hashLogJob: [ string, (hashResult: HashLogResult) => void ];
  let csvWriteJob: [ string, any[][], (err?: any) => void];
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
    while((availableWorkers.length > 0) && (convertLogQueue.length > 0)) {
      availableWorker = removeWorker();
      convertLogJob = convertLogQueue.shift();
      runningConvertLogJobs.push(convertLogJob);
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
  }
}

function initWorkerThread() {
  parentPort.on('message', (msg: WorkerMessage) => {
    switch(msg.messageType) {
      case MESSAGE_TYPES.ACK:
        parentPort.postMessage({
          messageType: MESSAGE_TYPES.ACK,
        });
        break;
      case MESSAGE_TYPES.PARSE_CSV:
        const csvPath = (msg?.data?.csvPath as string);
        if(csvPath === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(msg);
        } else {
          parseCsvLog(csvPath)
            .then(parseResult => {
              parentPort.postMessage({
                messageType: MESSAGE_TYPES.PARSE_CSV_COMPLETE,
                data: {
                  parseResult,
                  csvPath,
                },
              });
            });
        }
        break;
      case MESSAGE_TYPES.CONVERT_CSV:
        const csvPathDate = (msg?.data?.csvPathDate as CsvPathDate);
        if(csvPathDate === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(msg);
        } else {
          convertCsvPathDate(csvPathDate)
            .then(convertResult => {
              parentPort.postMessage({
                messageType: MESSAGE_TYPES.CONVERT_CSV_COMPLETE,
                data: {
                  csvPathDate,
                  convertResult,
                }
              });
            });
        }
        break;
      case MESSAGE_TYPES.CONVERT_CSV_LOG:
        const filePath = (msg?.data?.filePath as string);
        if(filePath === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(msg);
        } else {
          convertCsvLogFile(filePath)
            .then(convertLogResult => {
              parentPort.postMessage({
                messageType: MESSAGE_TYPES.CONVERT_CSV_LOG_COMPLETE,
                data: {
                  filePath,
                  convertLogResult,
                },
              });
            });
        }
        break;
      case MESSAGE_TYPES.HASH_FILE:
        const hashFilePath = (msg?.data?.filePath as string);
        if(hashFilePath === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(msg);
        } else {
          getFileHash(hashFilePath)
            .then(fileHash => {
              parentPort.postMessage({
                messageType: MESSAGE_TYPES.HASH_FILE_COMPLETE,
                data: {
                  filePath: hashFilePath,
                  fileHash,
                },
              });
            });
        }
        break;
      case MESSAGE_TYPES.CSV_WRITE:
        const targetCsvPath = (msg?.data?.targetCsvPath as string);
        const rows = (msg?.data?.rows as any[][]);
        if((targetCsvPath === undefined) || rows === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(`messageType: ${msg?.messageType}`);
          console.error(`targetCsvPath: ${msg?.data?.targetCsvPath}`);
        } else {
          writeCsv(targetCsvPath, rows)
            .then(() => {
              parentPort.postMessage({
                messageType: MESSAGE_TYPES.CSV_WRITE_COMPLETE,
                data: {
                  targetCsvPath,
                },
              });
            });
        }
        break;
      case MESSAGE_TYPES.EXIT:
        setTimeout(() => {
          process.exit();
        });
        break;
    }
  });
}
