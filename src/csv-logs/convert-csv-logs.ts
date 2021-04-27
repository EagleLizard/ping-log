
import path from 'path';

import { CONVERTED_CSV_LOGS_DIR_PATH, CSV_PING_LOG_DIR } from '../constants';
import { listDir } from '../lib/files';
import { printProgress } from '../print';
import { CsvPathDate, getCsvPathDates } from '../lib/date-time-util';
import { getHashesConcurrent } from './hash-log';
import { CsvLogMeta, _HashLogMetaValue } from './csv-log-meta';
import { CsvWriter } from '../lib/csv-writer';
import { initializePool, destroyWorkers, queueConvertCsvLog, } from '../csv-parse/worker-pool';
import { promises } from 'fs';
import { sleep } from '../lib/sleep';

export async function convertCsvLogs() {
  let csvPaths: string[], csvPathDates: CsvPathDate[];
  let startMs: number, endMs: number, deltaMs: number, deltaSeconds: number,
    deltaMinutes: number;
  let _hashLogMeta: _HashLogMetaValue[];
  let fileHashTuples: [ string, string ][];

  csvPaths = await listDir(CSV_PING_LOG_DIR);
  _hashLogMeta = await CsvLogMeta.getLogHashMeta();

  const pastDays = 7;
  // const pastMs = Date.now() - ((1000 * 60 * 60 * 24) * pastDays);
  const today = new Date;
  const pastMs = (new Date(today.getFullYear(), today.getMonth(), today.getDate() - pastDays)).valueOf();
  _hashLogMeta = _hashLogMeta.filter(metaVal => {
    return metaVal.timestamp < pastMs;
  });

  csvPathDates = getCsvPathDates(csvPaths, _hashLogMeta);

  // csvPathDates = csvPathDates.slice(-6);

  // console.log(hashLogMeta);

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

  await concurrentConvertCsvLogsByDate(csvPathDates, _hashLogMeta);

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

async function concurrentConvertCsvLogsByDate(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]) {
  let recordCount: number;
  let totalFiles: number, fileDoneCount: number;
  let fileHashTuples: [ string, string ][];

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
    let filePaths: string[], csvWriter: CsvWriter;
    let convertLogJobPromises: Promise<void>[];
    let headers: any[], recordIdCounter: number;

    headers = [
      // 'id',
      'time_stamp',
      'uri',
      'ping_ms'
    ];

    csvConvertFileName = `${csvPathDate.dateStr}.csv`;
    csvConvertFilePath = `${CONVERTED_CSV_LOGS_DIR_PATH}/${csvConvertFileName}`;

    filePaths = csvPathDate.csvPaths;
    convertLogJobPromises = [];
    recordIdCounter = 0;

    csvWriter = new CsvWriter(csvConvertFilePath);
    csvWriter.write(headers);

    for(let k = 0, filePath: string; filePath = filePaths[k], k < filePaths.length; ++k) {
      let convertLogPromise: Promise<void>;

      if(k !== 0) {
        await sleep(192);
      }

      convertLogPromise = queueConvertCsvLog(filePath)
        .then(convertLogResult => {
          fileHashTuples.push([
            convertLogResult.filePath,
            convertLogResult.fileHash,
          ]);
          return (async () => {

            for(let m = 0; m < convertLogResult.records.length; ++m) {
              await csvWriter.write(convertLogResult.records[m]);
              delete convertLogResult.records[m];
            }

            fileDoneCount++;
            recordCount += convertLogResult.recordCount;
            printProgress(fileDoneCount, totalFiles);
          })();
        });
      convertLogJobPromises.push(convertLogPromise);

    }
    await Promise.all(convertLogJobPromises);

    await csvWriter.end();

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

  await CsvLogMeta._writeHashMeta(hashLogMeta);

  await destroyWorkers();
  console.log(`\nrecordCount: ${recordCount.toLocaleString()}`);
  process.stdout.write('\n');
}

function convertRecord(record: any[]): any[] {
  // let parsedTimestamp: Date, parsedMs: number;
  // const [ id, timestamp, uri, ms ] = record;
  // parsedTimestamp = new Date(record[1]);
  // parsedMs = +record[3];
  record[1] = (new Date(record[1])).valueOf();
  record[3] = +record[3];
  return record;
  // return [
  //   record[0],
  //   parsedTimestamp.valueOf(),
  //   record[2],
  //   parsedMs,
  // ];
}
