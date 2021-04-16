
import sourceMapSupport from 'source-map-support';
sourceMapSupport.install();

import { Worker, isMainThread, parentPort } from 'worker_threads';
import os from 'os';

import { sleep } from '../lib/sleep';
import { CsvLogParseResult, parseCsvLog } from './parse-csv-log';
import { CsvConvertResult } from '../csv-logs/convert-csv-log';
import { CsvPathDate } from '../lib/date-time-util';

const NUM_CPUS = os.cpus().length;
const NUM_WORKERS = Math.round(
  NUM_CPUS
  // NUM_CPUS * 2
  // NUM_CPUS * 4
  // NUM_CPUS * 8
  // NUM_CPUS - 1
  // NUM_CPUS - 2
  // NUM_CPUS / 2
);

enum MESSAGE_TYPES {
  ACK = 'ack',
  EXIT = 'exit',
  PARSE_CSV = 'parse_csv',
  PARSE_CSV_COMPLETE = 'parse_csv_complete',
  CONVERT_CSV = 'convert_csv',
  CONVERT_CSV_COMPLETE = 'convert_csv_complete',
}

let workers: Worker[] = [];
let availableWorkers: Worker[] = [];
let parseQueue: [ string, (parseResult: CsvLogParseResult) => void ][] = [];
let runningParseJobs: [ string, (parseResult: CsvLogParseResult) => void ][] = [];
let convertQueue: [ CsvPathDate, (convertResult: CsvConvertResult) => void ][] = [];
let runningConvertJobs: [ CsvPathDate, (parseResult: CsvConvertResult) => void ][] = [];
let isInitialized = false;
let areWorkersDestroyed = false;

interface WorkerMessage {
  messageType: MESSAGE_TYPES,
  data?: any,
}

if(!isMainThread) {
  initWorkerThread();
}

(async () => {
  try {
    // await initializePool();
  } catch(e) {
    console.error(e);
    throw e;
  }
})();

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
        // case MESSAGE_TYPES.CONVERT_CSV_COMPLETE:
        //   let foundConvertJobIdx: number;
        //   let foundConvertJob: [ CsvPathDate, (convertResult: CsvConvertResult) => void ];
          // foundConvertJobIdx = runningConvertJobs.findIndex((convertJob) => {
          //   return convertJob[0].
          // });
      }
    });
  });

  checkQueueLoop();
}

function checkQueueLoop() {
  setTimeout(() => {
    let availableWorker: Worker, parseJob: [ string, (parseResult: CsvLogParseResult) => void ];
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
      case MESSAGE_TYPES.EXIT:
        setTimeout(() => {
          process.exit();
        });
        break;
    }
  });
}
