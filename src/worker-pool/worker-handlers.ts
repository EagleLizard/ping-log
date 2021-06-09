
import { parentPort } from 'worker_threads';

import {
  MESSAGE_TYPES,
} from './worker-constants';
import { WorkerMessage } from './worker-message';
import { CsvWriter, writeCsv } from '../lib/csv-writer';
import { sleep } from '../lib/sleep';
import { parseCsvLog } from '../csv-parse/parse-csv-log';
import { convertCsvPathDate } from '../csv-logs/convert-csv-path-date';
import { CsvPathDate } from '../lib/date-time-util';
import { readCsvLog } from '../csv-parse/read-csv-log';
import { convertCsvLogFile } from '../csv-logs/convert-csv-log';
import { getFileHash } from '../csv-logs/hash-log';

export const ASYNC_READ_RECORD_QUEUE_SIZE = Math.round(
  // 0.128e3
  // 0.256e3
  // 512
  // 1024
  // 2048
  // 4096
  // 8192

  // 1.024e4
  1.6384e4
  // 3.2768e4
  // 6.5536e4
  // 8.056e4
  // 1.6384e4
  // 1.024e5
);

let asyncCsvWriter: CsvWriter;

export function handleAck(msg: WorkerMessage) {
  parentPort.postMessage({
    messageType: MESSAGE_TYPES.ACK,
  });
}

export function handleParseCsv(msg: WorkerMessage) {
  const csvPath = (msg?.data?.csvPath as string);
  if(csvPath === undefined) {
    console.error('Error, malformed message sent to worker:');
    console.error(msg);
  } else {
    sleep(0).then(() => {
      return parseCsvLog(csvPath)
        .then(parseResult => {
          parentPort.postMessage({
            messageType: MESSAGE_TYPES.PARSE_CSV_COMPLETE,
            data: {
              parseResult,
              csvPath,
            },
          });
        });
    });
  }
}

export function handleConvertCsv(msg: WorkerMessage) {
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
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
v v v v v v v v v v v v v v v v v v v v v

*/

export function handleCsvRead(msg: WorkerMessage) {
  let recordCb: (record: any[], idx: number) => void;
  const readCsvPath = (msg?.data?.csvPath as string);
  const recordReadQueue: any[][] = [];
  const clearReadQueue = () => {
    parentPort.postMessage({
      messageType: MESSAGE_TYPES.ASYNC_CSV_READ,
      data: {
        csvPath: readCsvPath,
        records: recordReadQueue,
      }
    });
    recordReadQueue.length = 0;
  };
  recordCb = (record, idx) => {
    recordReadQueue.push(record);
    if(recordReadQueue.length > ASYNC_READ_RECORD_QUEUE_SIZE) {
      clearReadQueue();
    }
  };
  if(readCsvPath === undefined) {
    console.error('Error, malformed message sent to worker:');
    console.error(msg);
  } else {
    readCsvLog(readCsvPath, recordCb).then(csvReadResult => {
      clearReadQueue();
      parentPort.postMessage({
        messageType: MESSAGE_TYPES.CSV_READ_COMPLETE,
        data: {
          csvPath: readCsvPath,
          readResult: csvReadResult,
        },
      });
    });
  }
}

export function handleCsvWriter(msg: WorkerMessage) {
  const csvWriterPath = (msg?.data?.csvPath as string);
  if(csvWriterPath === undefined) {
    console.error('Error, malformed message sent to worker:');
    console.error(msg);
  } else {
    asyncCsvWriter = new CsvWriter(csvWriterPath);
    parentPort.postMessage({
      messageType: MESSAGE_TYPES.CSV_WRITER,
      data: {
        csvPath: csvWriterPath,
      },
    })
  }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
v v v v v v v v v v v v v v v v v v v v v

*/

export function handleCsvWriterEnd(msg: WorkerMessage) {
  if(asyncCsvWriter !== undefined) {
    (async () => {
      await asyncCsvWriter.end();
      asyncCsvWriter = undefined;
      parentPort.postMessage({
        messageType: MESSAGE_TYPES.CSV_WRITER_END,
      });
    })();
  }
}

export function handleCsvWriterWrite(msg: WorkerMessage) {
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
}

export function handleConvertCsvLog(msg: WorkerMessage) {
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
          records: recordQueue,
        },
      });
      recordQueue.length = 0;
    };
    const checkClear = () => {
      if(recordQueue.length > ASYNC_READ_RECORD_QUEUE_SIZE) {
        return clearRecordQueue();
      }
    };
    const recordCb = (record: any[]) => {
      recordQueue.push(record);
      checkClear();
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
}

export function handleHashFile(msg: WorkerMessage) {
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
}

export function handleCsvWrite(msg: WorkerMessage) {
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
}

export function handleExit(msg: WorkerMessage) {
  setTimeout(() => {
    process.exit();
  });
}
