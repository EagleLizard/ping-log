
import sourceMapSupport from 'source-map-support';
sourceMapSupport.install();

import { Worker, isMainThread, parentPort } from 'worker_threads';
import { sleep } from '../lib/sleep';


import {
  MESSAGE_TYPES,
  MAX_CSV_WRITERS,
  NUM_WORKERS,
} from './worker-constants';
import { WorkerMessage } from './worker-message';
import {
  workers,
  AsyncCsvWriter,
  checkQueues,
  CsvWriterJobTuple,
  getRunningWriterJob,
  queueCsvWriterJob,
  removeRunningCsvWriterJob,
  addWorker,
  handleParseCsvComplete,
  handleConverCsvComplete,
  handleCsvReadComplete,
  handleAsyncCsvRead,
  handleCsvWriterMain,
  handleConvertCsvLogRecordRead,
  handleConvertCsvLogComplete,
  handleHashFileComplete,
  handleCsvWriteComplete,
} from './main-thread-handlers';
import {
  ASYNC_READ_RECORD_QUEUE_SIZE,
  handleAck,
  handleConvertCsv,
  handleConvertCsvLog,
  handleCsvRead,
  handleCsvWrite,
  handleCsvWriter,
  handleCsvWriterEnd,
  handleCsvWriterWrite,
  handleExit,
  handleHashFile,
  handleParseCsv,
} from './worker-handlers';

export {
  queueHashJob,
  queueParseCsv,
  queueConvertCsv,
  queueConvertCsvLog,
  queueCsvReadJob,
} from './main-thread-handlers';

let isInitialized = false;
let areWorkersDestroyed = false;

if(!isMainThread) {
  initWorkerThread();
}

export async function getAsyncCsvWriter(filePath: string): Promise<AsyncCsvWriter> {
  let isWriting: boolean;
  let writerJob: CsvWriterJobTuple, writerWorker: Worker;

  await queueCsvWriterJob(filePath);
  writerJob = getRunningWriterJob(filePath);
  writerWorker = writerJob[2]();

  if(writerWorker === undefined) {
    throw new Error(`undefined worker for job ${filePath}`);
  }
  await new Promise<void>(resolve => {
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
        if(msg?.messageType === MESSAGE_TYPES.CSV_WRITER_END) {
          removeRunningCsvWriterJob(filePath, writerWorker);
          resolve();
        }
      });
    });
  }
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
          addWorker(worker);
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
          handleParseCsvComplete(msg, worker);
          break;
        case MESSAGE_TYPES.CONVERT_CSV_COMPLETE:
          handleConverCsvComplete(msg, worker);
          break;
        case MESSAGE_TYPES.CSV_READ_COMPLETE:
          handleCsvReadComplete(msg, worker);
          break;
        case MESSAGE_TYPES.ASYNC_CSV_READ:
          handleAsyncCsvRead(msg);
          break;
        case MESSAGE_TYPES.CSV_WRITER:
          handleCsvWriterMain(msg);
          break;
        case MESSAGE_TYPES.CONVERT_CSV_LOG_RECORD_READ:
          handleConvertCsvLogRecordRead(msg);
          break;
        case MESSAGE_TYPES.CONVERT_CSV_LOG_COMPLETE:
          handleConvertCsvLogComplete(msg, worker);
          break;
        case MESSAGE_TYPES.HASH_FILE_COMPLETE:
          handleHashFileComplete(msg, worker);
          break;
        case MESSAGE_TYPES.CSV_WRITE_COMPLETE:
          handleCsvWriteComplete(msg, worker);
          break;
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
      case MESSAGE_TYPES.CSV_READ:
        handleCsvRead(msg);
        break;
      case MESSAGE_TYPES.CSV_WRITER:
        handleCsvWriter(msg);
        break;
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
