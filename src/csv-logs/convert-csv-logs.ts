
import os from 'os';
import path from 'path';

import _chunk from 'lodash.chunk';

import { CONVERTED_CSV_LOGS_DIR_PATH, CSV_PING_LOG_DIR } from '../constants';
import { listDir } from '../lib/files';
import { printProgress } from '../print';
import { sleep } from '../lib/sleep';
import { getCsvDateMap, parseCsvLogFileDate, CsvPathDate } from '../lib/date-time-util';
import { getFileHash, getHashes, getHashesConcurrent } from './hash-log';
import { CsvLogMeta, _HashLogMetaValue } from './csv-log-meta';
import { CsvWriter, writeCsv } from '../lib/csv-writer';
import { scanLog } from '../lib/csv-read';
import { convertCsvPathDate, CsvConvertResult } from './convert-csv-path-date';
import { convertCsvLogFile, CsvLogConvertResult } from './convert-csv-log';
import { initializePool, destroyWorkers, queueConvertCsv, queueConvertCsvLog } from '../csv-parse/worker-pool';

const NUM_CPUS = os.cpus().length;
const CSV_CHUNK_SIZE = Math.round(
  // 1
  NUM_CPUS - 1
  // NUM_CPUS * 4
  // NUM_CPUS * Math.E
  // NUM_CPUS * Math.LOG2E
  // NUM_CPUS / Math.E
  // NUM_CPUS / Math.LOG2E
  // NUM_CPUS / 4
);

// 185577480

const PER_FILE_SLEEP_MS = 0;
const PER_CHUNK_SLEEP_MS = 8;
const PER_DATE_SLEEP_MS = 32;

const WEEK_AGO_MS = Date.now() - (1000 * 60 * 60 * 24 * 7);

export async function convertCsvLogs() {
  // console.log(`NUM_CPUS: ${NUM_CPUS}`);
  // console.log(`CHUNK SIZE: ${CSV_CHUNK_SIZE}`);
  // console.log(`PER_FILE_SLEEP_MS: ${PER_FILE_SLEEP_MS}`);
  // console.log(`PER_CHUNK_SLEEP_MS: ${PER_CHUNK_SLEEP_MS}`);
  // console.log(`PER_DATE_SLEEP_MS: ${PER_DATE_SLEEP_MS}`);

  let csvPaths: string[], csvDateMap: Map<string, string[]>,
    csvPathDates: CsvPathDate[];
  let startMs: number, endMs: number, deltaMs: number, deltaSeconds: number,
    deltaMinutes: number;
  let _hashLogMeta: _HashLogMetaValue[];
  let fileHashTuples: [ string, string ][];

  csvPaths = await listDir(CSV_PING_LOG_DIR);

  csvDateMap = getCsvDateMap(csvPaths);
  csvPathDates = [ ...csvDateMap ].map(csvDateTuple => {
    let parsedDate: Date, dateMs: number, csvPathDate: CsvPathDate;
    parsedDate = parseCsvLogFileDate(csvDateTuple[0]);
    dateMs = parsedDate.valueOf();
    csvPathDate = {
      date: parsedDate,
      dateMs,
      dateStr: csvDateTuple[0],
      csvPaths: csvDateTuple[1],
    };
    return csvPathDate;
  });
  csvPathDates.sort((a, b) => {
    if(a.dateMs > b.dateMs) {
      return 1;
    }
    if(a.dateMs < b.dateMs) {
      return -1;
    }
    return 0;
  });
  // pluck out today as it's likely still being appended
  const nowDate = new Date;
  const todayCsvPathDateIdx = csvPathDates.findIndex(csvPathDate => {
    return (nowDate.getFullYear() === csvPathDate.date.getFullYear())
      && (nowDate.getMonth() === csvPathDate.date.getMonth())
      && (nowDate.getDate() === csvPathDate.date.getDate())
    ;
  });

  if(todayCsvPathDateIdx !== -1) {
    csvPathDates.splice(todayCsvPathDateIdx, 1);
  }

  csvPathDates = csvPathDates.slice(-4);

  _hashLogMeta = await CsvLogMeta.getLogHashMeta();

  // console.log(hashLogMeta);

  /*
    only check limited hashes in the past, unless they aren't in the metadata
  */
  csvPathDates = csvPathDates.filter(csvPathDate => {
    let missingMeta: boolean;
    missingMeta = csvPathDate.csvPaths.some(csvPath => {
      let foundMeta: _HashLogMetaValue;
      foundMeta = _hashLogMeta.find(metaVal => {
        return metaVal.filePath === csvPath;
      });
      return foundMeta === undefined;
    });
    return missingMeta || (csvPathDate.date.valueOf() > WEEK_AGO_MS);
  });
  const csvPathFileSum = csvPathDates.reduce((acc, curr) => {
    return acc + curr.csvPaths.length;
  }, 0);
  console.log(`Total filePaths: ${csvPathFileSum}`);
  console.log(`Total pathDates: ${csvPathDates.length}`);
  console.log(csvPathDates.map(csvPathDate => csvPathDate.dateStr).join(', '));

  // fileHashTuples = await getHashes(csvPathDates, _hashLogMeta);
  fileHashTuples = await getHashesConcurrent(csvPathDates, _hashLogMeta);
  csvPathDates = CsvLogMeta.filterHashedCsvPathDates(csvPathDates, _hashLogMeta, fileHashTuples);
  const filteredCsvFileSum = csvPathDates.reduce((acc, curr) => {
    return acc + curr.csvPaths.length;
  }, 0);
  console.log(`Filtered filePaths: ${filteredCsvFileSum}`);

  await CsvLogMeta.initConvertedLogsDir();

  startMs = Date.now();

  // await convertCsvLogsByDate(csvPathDates, _hashLogMeta);
  // await _convertCsvLogsByDate(csvPathDates, _hashLogMeta);
  await concurrentConvertCsvLogsByDate(csvPathDates, _hashLogMeta);
  // await concurrentConvertPathDates(csvPathDates, _hashLogMeta);

  endMs = Date.now();

  deltaMs = endMs - startMs;
  deltaSeconds = deltaMs / 1000;
  deltaMinutes = deltaSeconds / 60;
  if(deltaMinutes < 1) {
    console.log(`Scan took ${deltaSeconds.toFixed(2)} seconds`);
  } else {
    console.log(`Scan took ${deltaMinutes.toFixed(2)} minutess`);
  }
  process.stdout.write('\n');
  await destroyWorkers();
}

async function concurrentConvertPathDates(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]) {
  let convertJobPromises: Promise<void>[];
  let doneCount: number, recordCount: number;
  let fileHashTuples: [ string, string ][];

  fileHashTuples = [];
  recordCount = 0;
  doneCount = 0;
  convertJobPromises = [];
  await initializePool();
  for(let i = 0, currPathDate: CsvPathDate; currPathDate = csvPathDates[i], i < csvPathDates.length; ++i) {
    let convertJobPromise: Promise<void>;
    convertJobPromise = queueConvertCsv(currPathDate)
      .then(convertResult => {
        fileHashTuples.push(...convertResult.fileHashTuples);
        recordCount += convertResult.recordCount;
        doneCount++;
        printProgress(doneCount, csvPathDates.length);
      });
    convertJobPromises.push(convertJobPromise);
  }
  await Promise.all(convertJobPromises);
  await destroyWorkers();
  console.log(`\nrecords: ${recordCount.toLocaleString()}\n`);
}

async function concurrentConvertCsvLogsByDate(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]) {
  let recordCount: number, writeTimesMs: number[], writeStartMs: number, writeEndMs: number;
  let totalFiles: number, fileDoneCount: number;
  let fileHashTuples: [ string, string ][];

  writeTimesMs = [];

  fileHashTuples = [];
  // doneCount = 0;
  recordCount = 0;
  fileDoneCount = 0;
  totalFiles = csvPathDates.reduce((acc, curr) => {
    return acc + curr.csvPaths.length;
  }, 0);
  await initializePool();
  for(let i = 0, csvPathDate: CsvPathDate; csvPathDate = csvPathDates[i], i < csvPathDates.length; ++i) {
    let csvConvertFileName: string, csvConvertFilePath: string;
    let filePaths: string[];
    let convertLogJobPromises: Promise<void>[];
    let headers: any[], records: any[][], recordIdCounter: number;
    let convertedRecords: any[][];
    headers = [ 'id', 'time_stamp', 'uri', 'ping_ms' ];

    csvConvertFileName = `${csvPathDate.dateStr}.csv`;
    csvConvertFilePath = `${CONVERTED_CSV_LOGS_DIR_PATH}/${csvConvertFileName}`;

    filePaths = csvPathDate.csvPaths;
    records = [];
    convertLogJobPromises = [];
    for(let k = 0, filePath: string; filePath = filePaths[k], k < filePaths.length; ++k) {
      let convertLogPromise: Promise<void>;
      convertLogPromise = queueConvertCsvLog(filePath)
        .then(convertLogResult => {
          fileHashTuples.push([
            convertLogResult.filePath,
            convertLogResult.fileHash,
          ]);
          for(let m = 0; m < convertLogResult.records.length; ++m) {
            records.push(convertLogResult.records[m]);
          }
          // while(convertLogResult.records.length) {
          //   records.push(
          //     convertLogResult.records.pop()
          //   );
          // }
          fileDoneCount++;
          recordCount += convertLogResult.recordCount;
          printProgress(fileDoneCount, totalFiles);
        });
      convertLogJobPromises.push(convertLogPromise);

    }
    await Promise.all(convertLogJobPromises);
    recordIdCounter = 0;
    convertedRecords = records;
    for(let k = 0; k < convertedRecords.length; ++k) {
      let recordId: number, recordWithId: any[], convertedRecord: any[];
      recordId = recordIdCounter++;
      convertedRecord = convertRecord([ recordId, ...convertedRecords[k] ]);
      convertedRecords[k] = convertedRecord;
    }
    // while(records.length) {
    //   let recordId: number;
    //   let dateRecord: any[], recordWithId: any[], convertedRecord: any[];
    //   dateRecord = records.pop();
    //   recordId = ++recordIdCounter;
    //   recordWithId = [ recordId, ...dateRecord ];
    //   convertedRecord = convertRecord(recordWithId);
    //   convertedRecords.push(convertedRecord);
    // }
    convertedRecords.sort((a, b) => {
      let aStamp: number, bStamp: number;
      aStamp = a[1];
      bStamp = b[1];
      if(aStamp > bStamp) {
        return 1;
      }
      if(aStamp < bStamp) {
        return -1;
      }
      return 0;
    });
    writeStartMs = Date.now();
    /* ---- */

    // const csvWriter = new CsvWriter(csvConvertFilePath);
    // csvWriter.write(headers);
    // for(let i = 0; i < convertedRecords.length; ++i) {
    //   csvWriter.write(convertedRecords[i]);
    // }
    // await sleep(1);
    // await csvWriter.end();

    await writeCsv(csvConvertFilePath, [
      headers,
      ...convertedRecords,
    ]);

    /* ---- */
    writeEndMs = Date.now();
    writeTimesMs.push(writeEndMs - writeStartMs);

  }

  fileHashTuples.forEach(fileHashTuple => {
    let foundHashMetaVal: _HashLogMetaValue;
    const key = path.parse(fileHashTuple[0]).base;
    foundHashMetaVal = hashLogMeta.find(metaVal => {
      return metaVal.fileKey === key;
    });
    if(foundHashMetaVal !== undefined) {
      foundHashMetaVal.filePath = fileHashTuple[0];
      foundHashMetaVal.fileHash = fileHashTuple[1];
    } else {
      hashLogMeta.push({
        fileKey: key,
        filePath: fileHashTuple[0],
        fileHash: fileHashTuple[1],
        timestamp: CsvLogMeta.getTimestampFromFilepath(fileHashTuple[0]).valueOf(),
      });
    }
  });

  // await CsvLogMeta._writeHashMeta(hashLogMeta);

  await destroyWorkers();
  console.log(`\nrecordCount: ${recordCount.toLocaleString()}`);
  const writeMsTotal = writeTimesMs.reduce((acc, curr) => {
    return acc + curr;
  }, 0);
  const writeMsAvg = writeMsTotal / writeTimesMs.length;
  console.log(writeTimesMs);
  console.log(`writeMs total: ${writeMsTotal.toLocaleString()}ms`);
  console.log(`writeMs avg: ${(+(writeMsAvg.toFixed(1))).toLocaleString()}ms`);
  process.stdout.write('\n');
}

async function _convertCsvLogsByDate(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]) {
  let convertResults: CsvConvertResult[];
  convertResults = [];
  for(let i = 0; i < csvPathDates.length; ++i) {
    let currPathDate: CsvPathDate;
    currPathDate = csvPathDates[i];
    const convertResult = await convertCsvPathDate(currPathDate);
    convertResults.push(convertResult);
    printProgress(i + 1, csvPathDates.length);
  }
  const recordTotal = convertResults.reduce((acc, curr) => {
    return acc + curr.recordCount;
  }, 0);
  console.log(`\nrecords: ${recordTotal.toLocaleString()}`);
}

async function convertCsvLogsByDate(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]) {
  let totalSize: number, totalSizeMb: number,
    byteTotal: number, byteConvertedTotal: number,
    mbTotal: number, mbConvertedTotal: number;
  let recordCount: number;
  let totalFileCount: number, completeCount: number;
  let fileHashTuples: [ string, string ][];
  let headers: string[];
  let recordIdCounter: number;
  headers = [ 'id', 'time_stamp', 'uri', 'ping_ms' ];

  fileHashTuples = [];

  totalFileCount = csvPathDates.reduce((acc, curr) => {
    return acc + curr.csvPaths.length;
  }, 0);
  completeCount = 0;

  recordCount = 0;
  byteTotal = 0;
  byteConvertedTotal = 0;

  csvPathDates = csvPathDates.filter(csvPathDate => {
    return csvPathDate.csvPaths.length > 0;
  });

  for(let i = 0, currCsvPathDate: CsvPathDate; currCsvPathDate = csvPathDates[i], i < csvPathDates.length; ++i) {
    // if(currCsvPathDate.csvPaths.length < 1) {
    //   continue;
    // }
    let csvChunks: string[][];
    let dayByteTotal: number, dayByteConvertedTotal: number;
    let dateRecords: any[][], convertedDateRecords: any[][];
    let csvConvertFileName: string, csvConvertFilePath: string, csvWriter: CsvWriter;

    dayByteTotal = 0;
    dayByteConvertedTotal = 0;

    csvConvertFileName = `${currCsvPathDate.dateStr}.csv`;
    csvConvertFilePath = `${CONVERTED_CSV_LOGS_DIR_PATH}/${csvConvertFileName}`;
    // csvWriter = new CsvWriter(csvConvertFilePath);

    dateRecords = [];
    convertedDateRecords = [];
    csvChunks = _chunk(currCsvPathDate.csvPaths, CSV_CHUNK_SIZE);

    for(let n = 0, currChunk: string[]; currChunk = csvChunks[n], n < csvChunks.length; ++n) {
      let scanLogPromises: Promise<void>[];
      scanLogPromises = [];
      for(let k = 0, currCsvPath: string; currCsvPath = currChunk[k], k < currChunk.length; ++k) {
        let scanLogPromise: Promise<void>;

        scanLogPromise = scanLog(currCsvPath, (record, recordIdx) => {
          if((recordIdx === 0) && (headers === undefined)) {
            if(
              headers[0] !== 'time_stamp'
              || headers[1] !== 'uri'
              || headers[2] !== 'ping_ms'
            ) {
              throw new Error(`Unexpected headers from source csv: ${headers.join(', ')}`);
            }
          } else {
            recordCount++;
          }
          if(recordIdx !== 0) {
            dateRecords.push(record);
          }
        }).then(() => {
          return getFileHash(currCsvPath).then(hash => {
            fileHashTuples.push([
              currCsvPath,
              hash,
            ]);
          });
        }).then(() => {
          completeCount++;
          printProgress(completeCount, totalFileCount);
          return sleep(PER_FILE_SLEEP_MS);
        });
        scanLogPromises.push(scanLogPromise);
      }
      await Promise.all(scanLogPromises);
      await sleep(PER_CHUNK_SLEEP_MS);
    }

    dateRecords.reverse();

    recordIdCounter = 0;

    while(dateRecords.length) {
      let dateRecord: any[], convertedRecord: any[];
      let recordId: number, recordWithId: any[];
      dateRecord = dateRecords.pop();
      recordId = ++recordIdCounter;
      recordWithId = [ recordId, ...dateRecord ];
      convertedRecord = convertRecord(recordWithId);
      byteTotal += dateRecord.join(',').length;
      byteConvertedTotal += convertedRecord.join(',').length;
      convertedDateRecords.push(convertedRecord);
    }
    convertedDateRecords.sort((a, b) => {
      let aStamp: number, bStamp: number;
      aStamp = a[1];
      bStamp = b[1];
      if(aStamp > bStamp) {
        return 1;
      }
      if(aStamp < bStamp) {
        return -1;
      }
      return 0;
    });

    await writeCsv(csvConvertFilePath, [
      headers,
      ...convertedDateRecords,
    ]);
    // await CsvLogMeta.writeLastId(recordIdCounter);

    byteTotal += dayByteTotal;
    byteConvertedTotal += dayByteConvertedTotal;
    await sleep(PER_DATE_SLEEP_MS);
  }

  fileHashTuples.forEach(fileHashTuple => {
    let foundHashMetaVal: _HashLogMetaValue;
    const key = path.parse(fileHashTuple[0]).base;
    foundHashMetaVal = hashLogMeta.find(metaVal => {
      return metaVal.fileKey === key;
    });
    if(foundHashMetaVal !== undefined) {
      foundHashMetaVal.filePath = fileHashTuple[0];
      foundHashMetaVal.fileHash = fileHashTuple[1];
    } else {
      hashLogMeta.push({
        fileKey: key,
        filePath: fileHashTuple[0],
        fileHash: fileHashTuple[1],
        timestamp: CsvLogMeta.getTimestampFromFilepath(fileHashTuple[0]).valueOf(),
      });
    }
  });

  // await CsvLogMeta._writeHashMeta(hashLogMeta);

  mbTotal = byteTotal / 1024 / 1024;
  mbConvertedTotal = byteConvertedTotal / 1024 / 1024;
  process.stdout.write('\n');
  process.stdout.write(`\nrecords: ${recordCount.toLocaleString()}`);
  process.stdout.write(`\nrecordIdCounter: ${recordIdCounter.toLocaleString()}`);
  process.stdout.write('\n');
  process.stdout.write(`original: ${mbTotal.toFixed(2)}mb`);
  process.stdout.write('\n');
  process.stdout.write(`converted: ${mbConvertedTotal.toFixed(2)}mb`);
  process.stdout.write('\n');
}

function convertRecord(record: any[]): any[] {
  let parsedTimestamp: Date, parsedMs: number;
  const [ id, timestamp, uri, ms ] = record;
  parsedTimestamp = new Date(timestamp);
  parsedMs = +ms;
  return [
    id,
    parsedTimestamp.valueOf(),
    uri,
    parsedMs,
  ];
}
