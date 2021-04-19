
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

const NUM_CPUS = os.cpus().length;
const NUM_WORKERS = Math.round(
  // 1
  NUM_CPUS - 1
  // NUM_CPUS / 2
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
            availableWorkers.push(worker);
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
            availableWorkers.push(worker);
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
            availableWorkers.push(worker);
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
            availableWorkers.push(worker);
            foundHashJob[1]({
              filePath: msg?.data?.filePath,
              fileHash: msg?.data?.fileHash,
            });
          }

          break;
      }
    });
  });

  checkQueueLoop();
}

function checkQueueLoop() {
  setTimeout(() => {
    let availableWorker: Worker, parseJob: [ string, (parseResult: CsvLogParseResult) => void ];
    let convertJob: [ CsvPathDate, (convertResult: CsvConvertResult) => void ];
    let convertLogJob: [ string, (convertLogResult: CsvLogConvertResult) => void ];
    let hashLogJob: [ string, (hashResult: HashLogResult) => void ];
    while((parseQueue.length > 0) && (availableWorkers.length > 0)) {
      availableWorker = availableWorkers.pop();
      parseJob = parseQueue.shift();
      runningParseJobs.push(parseJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.PARSE_CSV,
        data: {
          csvPath: parseJob[0],
        },
      });
    }
    while((convertQueue.length > 0) && (availableWorkers.length > 0)) {
      availableWorker = availableWorkers.pop();
      convertJob = convertQueue.shift();
      runningConvertJobs.push(convertJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CONVERT_CSV,
        data: {
          csvPathDate: convertJob[0],
        },
      });
    }
    while((convertLogQueue.length > 0) && (availableWorkers.length > 0)) {
      availableWorker = availableWorkers.pop();
      convertLogJob = convertLogQueue.shift();
      runningConvertLogJobs.push(convertLogJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.CONVERT_CSV_LOG,
        data: {
          filePath: convertLogJob[0],
        },
      });
    }

    while((hashQueue.length > 0) && (availableWorkers.length > 0)) {
      availableWorker = availableWorkers.pop();
      hashLogJob = hashQueue.shift();
      runningHashJobs.push(hashLogJob);
      availableWorker.postMessage({
        messageType: MESSAGE_TYPES.HASH_FILE,
        data: {
          filePath: hashLogJob[0],
        },
      });
    }

    if(!areWorkersDestroyed) {
      checkQueueLoop();
    }
  }, 10);
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
      case MESSAGE_TYPES.EXIT:
        setTimeout(() => {
          process.exit();
        });
        break;
    }
  });
}
