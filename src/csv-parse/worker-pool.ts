
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
import { CsvWriter, writeCsv } from '../lib/csv-writer';

const NUM_CPUS = os.cpus().length;
const NUM_WORKERS = Math.round(
  // 2
  // NUM_CPUS - 2
  // NUM_CPUS - 1
  // NUM_CPUS
  NUM_CPUS / Math.LOG2E
  // NUM_CPUS / Math.E
  // NUM_CPUS / 2
  // NUM_CPUS / 4
  // NUM_CPUS * 2
  // NUM_CPUS * 4
  // NUM_CPUS * 8
  // NUM_CPUS - 2
);

const ASYNC_READ_RECORD_QUEUE_SIZE = Math.round(
  // 1024
  // 1.375e4
  // 1.75e4

  // 8.192e4
  // 1.6384e4
  // 2.4576e4
  3.2768e4
  // 4.9152e4
  // 6.5536e4
  // 1.31072e5
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
  CONVERT_CSV_LOG_RECORD_READ = 'convert_csv_log_record_read',
  CSV_WRITER_INIT = 'csv_writer_init',
  CSV_WRITER_WRITE = 'csv_writer_write',
  CSV_WRITER_WRITE_DONE = 'csv_writer_write_done',
  CSV_WRITER_END = 'csv_writer_end',
  CSV_WRITE = 'csv_write',
  CSV_WRITE_COMPLETE = 'csv_write_complete'
}

let workers: Worker[] = [];
let availableWorkers: Worker[] = [];

let asyncCsvWriterWorker: Worker;
let asyncCsvWriter: CsvWriter;

let hashQueue: [ string, (hashResult: HashLogResult) => void ][] = [];
let runningHashJobs: [ string, (hashResult: HashLogResult) => void ][] = [];

let parseQueue: [ string, (parseResult: CsvLogParseResult) => void ][] = [];
let runningParseJobs: [ string, (parseResult: CsvLogParseResult) => void ][] = [];

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

let convertRecordReadCount = 0;

export interface AsyncCsvWriter {
  write: (records: any[][]) => Promise<void>;
  end: () => Promise<void>;
}

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

export async function getAsyncCsvWriter(filePath: string): Promise<AsyncCsvWriter> {
  let isWriting: boolean;
  while(availableWorkers.length < 1) {
    await sleep(1);
  }
  asyncCsvWriterWorker = availableWorkers.pop();
  asyncCsvWriterWorker.postMessage({
    messageType: MESSAGE_TYPES.CSV_WRITER_INIT,
    data: {
      filePath,
    }
  });
  await new Promise<void>((resolve, reject) => {
    asyncCsvWriterWorker.once('message', msg => {
      if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER_INIT) {
        resolve();
      } else {
        reject(msg);
      }
    });
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
    asyncCsvWriterWorker.postMessage({
      messageType: MESSAGE_TYPES.CSV_WRITER_WRITE,
      data: {
        records,
      },
    });
    // records.length = 0;
    await new Promise<void>(resolve => {
      asyncCsvWriterWorker.once('message', msg => {
        if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER_WRITE_DONE) {
          isWriting = false;
          resolve();
        }
      });
    });
  }

  async function end() {
    asyncCsvWriterWorker.postMessage({
      messageType: MESSAGE_TYPES.CSV_WRITER_END,
    });
    return new Promise<void>(resolve => {
      asyncCsvWriterWorker.once('message', msg => {
        if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER_END) {
          availableWorkers.push(asyncCsvWriterWorker);
          asyncCsvWriterWorker = undefined;
          resolve();
        }
      });
    });
  }
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
  console.log(`ASYNC_READ_RECORD_QUEUE_SIZE: ${ASYNC_READ_RECORD_QUEUE_SIZE.toLocaleString()}`);
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
  if(availableWorkers.length > 0) {
    if(!queueLoop) {
      // console.log(`availableWOrkers: ${availableWorkers.length}`);
      // console.log(`convertQueue: ${convertLogQueue.length}`);
    }
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
      /*

      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      v v v v v v v v v v v v v v v v v v v v v

    */
      case MESSAGE_TYPES.CSV_WRITER_INIT:
        const asyncCsvWriterPath = (msg?.data?.filePath as string);
        if(asyncCsvWriterPath === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(msg);
        } else {
          asyncCsvWriter = new CsvWriter(asyncCsvWriterPath);
          parentPort.postMessage({
            messageType: MESSAGE_TYPES.CSV_WRITER_INIT,
          });
        }
        break;
      case MESSAGE_TYPES.CSV_WRITER_END:
        if(asyncCsvWriter !== undefined) {
          (async () => {
            await asyncCsvWriter.end();
            asyncCsvWriter = undefined;
            parentPort.postMessage({
              messageType: MESSAGE_TYPES.CSV_WRITER_END,
            });
          })();
        }
        break;
      case MESSAGE_TYPES.CSV_WRITER_WRITE:
        const asyncCsvWriteRecords = (msg?.data?.records as any[][]);
        if(!Array.isArray(asyncCsvWriteRecords)) {
          console.error('Error, malformed message sent to worker:');
          console.log(msg?.messageType);
        } else {
          (async () => {
            // while(asyncCsvWriteRecords.length > 0) {
            //   await asyncCsvWriter.write(asyncCsvWriteRecords.pop());
            // }
            for(let i = 0; i < asyncCsvWriteRecords.length; ++i) {
              await asyncCsvWriter.write(asyncCsvWriteRecords[i]);
            }
            parentPort.postMessage({
              messageType: MESSAGE_TYPES.CSV_WRITER_WRITE_DONE,
            });
          })();
        }
        // asyncCsvWriter.write();
        break;
      case MESSAGE_TYPES.CONVERT_CSV_LOG:
        const filePath = (msg?.data?.filePath as string);
        if(filePath === undefined) {
          console.error('Error, malformed message sent to worker:');
          console.error(msg);
        } else {
          const recordQueue: any[][] = [];
          const clearRecordQueue = () => {
            parentPort.postMessage({
              messageType: MESSAGE_TYPES.CONVERT_CSV_LOG_RECORD_READ,
              data: {
                filePath,
                records: recordQueue.slice(),
              },
            });
            recordQueue.length = 0;
          };
          const recordCb = (record: any[]) => {
            recordQueue.push(record);
            if(recordQueue.length > ASYNC_READ_RECORD_QUEUE_SIZE) {
              clearRecordQueue();
            }
          };
          sleep(0).then(() => {
            return convertCsvLogFile(filePath, recordCb)
              .then(convertLogResult => {
                if(recordQueue.length > 0) {
                  clearRecordQueue();
                }
                return convertLogResult;
              })
              .then(convertLogResult => {
                parentPort.postMessage({
                  messageType: MESSAGE_TYPES.CONVERT_CSV_LOG_COMPLETE,
                  data: {
                    filePath,
                    convertLogResult,
                  },
                });
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
