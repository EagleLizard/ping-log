
import path from 'path';

import { CONVERTED_CSV_LOGS_DIR_PATH, CSV_PING_LOG_DIR, CONVERTED_CSV_LOGS_EXT_DIR_PATH } from '../constants';
import { listDir } from '../lib/files';
import { printProgress, getIntuitiveTimeFromMs } from '../print';
import { CsvPathDate, getCsvPathDates, isDateInRange } from '../lib/date-time-util';
import { getHashesConcurrent } from './hash-log';
import { CsvLogMeta, _HashLogMetaValue } from './csv-log-meta';
import { CsvWriter } from '../lib/csv-writer';
import { initializePool, destroyWorkers, queueConvertCsvLog, AsyncCsvWriter, getAsyncCsvWriter, } from '../csv-parse/worker-pool';
import { sleepImmediate } from '../lib/sleep';
import { Timer } from '../lib/timer';

const USE_EXT_DIR = false;

let USE_TEST_DATES: boolean;
USE_TEST_DATES = false;

function getTestDates() {
  const today = new Date;

  const pastDays = 2;

  const daysSinceNewInternet = (
    (
      (new Date(today.getFullYear(), today.getMonth(), today.getDate()))
        .valueOf() / 1000 / 60 / 60 / 24
    ) - (
      (new Date('2020-10-23')
        .valueOf() / 1000 / 60 / 60 / 24)
    )
  );
  // const daysInPast = daysSinceNewInternet + 30;
  // const daysInPast = 1;
  const daysInPast = 15;
  // const daysInPast = 60;
  // const daysInPast = 187;
  // const daysInPast = 185;
  // const daysInPast = 200;
  // const pastMs = Date.now() - ((1000 * 60 * 60 * 24) * pastDays);


  const maxDate = new Date(
    today.getFullYear(),
    today.getMonth(),
    today.getDate() - daysInPast,
  );
  const minDate = new Date(
    maxDate.getFullYear(),
    maxDate.getMonth(),
    maxDate.getDate() - pastDays,
  );
  return {
    today,
    maxDate,
    minDate,
  };
}

function testFilterMeta(_hashLogMeta: _HashLogMetaValue[], minDate: Date): _HashLogMetaValue[] {
  if(USE_TEST_DATES === false) {
    return _hashLogMeta;
  }
  _hashLogMeta = _hashLogMeta.filter(metaVal => {
    let metaDate: Date;
    metaDate = new Date(metaVal.timestamp);
    return metaDate < minDate;
  });
  return _hashLogMeta;
}

function testFilterPathDates(csvPathDates: CsvPathDate[], minDate: Date, maxDate: Date): CsvPathDate[] {
  if(USE_TEST_DATES === false) {
    return csvPathDates;
  }
  csvPathDates = csvPathDates.filter(csvPathDate => {
    return isDateInRange(csvPathDate.date, [ minDate, maxDate ]);
  });
  return csvPathDates;
}

export async function convertCsvLogs() {
  let csvPaths: string[], csvPathDates: CsvPathDate[];
  let convertTimer: Timer;
  let startMs: number, endMs: number, deltaMs: number, deltaSeconds: number,
    deltaMinutes: number;
  let _hashLogMeta: _HashLogMetaValue[];
  let fileHashTuples: [ string, string ][];

  csvPaths = await listDir(CSV_PING_LOG_DIR);
  _hashLogMeta = await CsvLogMeta.getLogHashMeta();

  const testDates = getTestDates();
  _hashLogMeta = testFilterMeta(_hashLogMeta, testDates.minDate);

  csvPathDates = getCsvPathDates(csvPaths, _hashLogMeta);

  csvPathDates = testFilterPathDates(csvPathDates, testDates.minDate, testDates.maxDate);

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

  convertTimer = Timer.start();

  await concurrentConvertCsvLogsByDate(csvPathDates, _hashLogMeta);

  deltaMs = convertTimer.stop();
  deltaSeconds = deltaMs / 1000;
  deltaMinutes = deltaSeconds / 60;
  if(deltaMinutes < 1) {
    console.log(`Scan took ${deltaSeconds.toFixed(2)} seconds`);
  } else {
    console.log(`Scan took ${deltaMinutes.toFixed(2)} minutes`);
  }
  process.stdout.write('\n');
  await destroyWorkers();
}

async function concurrentConvertCsvLogsByDate(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]) {
  let recordCount: number;
  let totalFiles: number, fileDoneCount: number;
  let fileHashTuples: [ string, string ][];
  let convertRecordReadCount: number;
  let convertLogTimesMs: number[], readLogTimesMs: number[];
  convertRecordReadCount = 0;
  convertLogTimesMs = [];
  readLogTimesMs = [];
  //filter path dates with no files to convertz
  csvPathDates = csvPathDates.filter(csvPathDate => {
    return csvPathDate.csvPaths.length > 0;
  });

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
    let filePaths: string[], csvWriter: CsvWriter, asyncCsvWriter: AsyncCsvWriter;
    let convertLogJobPromises: Promise<void>[];
    let headers: any[], recordIdCounter: number;
    let convertTimer: Timer;

    headers = [
      // 'id',
      'time_stamp',
      'uri',
      'ping_ms'
    ];

    csvConvertFileName = `${csvPathDate.dateStr}.csv`;
    if(USE_EXT_DIR) {
      csvConvertFilePath = `${CONVERTED_CSV_LOGS_EXT_DIR_PATH}/${csvConvertFileName}`;
    } else {
      csvConvertFilePath = `${CONVERTED_CSV_LOGS_DIR_PATH}/${csvConvertFileName}`;
    }

    filePaths = csvPathDate.csvPaths;
    convertLogJobPromises = [];
    recordIdCounter = 0;

    // csvWriter = new CsvWriter(csvConvertFilePath);
    // csvWriter.write(headers);

    asyncCsvWriter = await getAsyncCsvWriter(csvConvertFilePath);
    await asyncCsvWriter.write([ headers ]);

    convertTimer = Timer.start();

    for(let k = 0, filePath: string; filePath = filePaths[k], k < filePaths.length; ++k) {
      let convertLogPromise: Promise<void>;
      let readLogTimer: Timer;

      if(k !== 0) {
        // await sleep(32);
        // await sleep(64);
        // await sleep(128);
        // await sleep(256);
      }
      let asyncWrites = 0;

      const recordsCb = async (records: any[][]) => {
        convertRecordReadCount = convertRecordReadCount + records.length;
        asyncWrites++;
        await asyncCsvWriter.write(records);
        asyncWrites--;
      };

      const recordStartCb = () => {
        readLogTimer = Timer.start();
      };

      // readLogTimer = Timer.start();

      convertLogPromise = queueConvertCsvLog(filePath, recordsCb, recordStartCb)
        .then(convertLogResult => {
          fileHashTuples.push([
            convertLogResult.filePath,
            convertLogResult.fileHash,
          ]);
          return (async () => {

            // for(let m = 0; m < convertLogResult.records.length; ++m) {
            //   await csvWriter.write(convertLogResult.records[m]);
            //   delete convertLogResult.records[m];
            // }
            while(asyncWrites > 0) {
              // console.log(`asyncWrites: ${asyncWrites}`);
              // await sleep(0);
              await sleepImmediate();
            }
            readLogTimesMs.push(readLogTimer.stop());
            fileDoneCount++;
            recordCount += convertLogResult.recordCount;
            printProgress(fileDoneCount, totalFiles);
          })();
        });
      convertLogJobPromises.push(convertLogPromise);

    }
    await Promise.all(convertLogJobPromises);

    // await csvWriter.end();

    await asyncCsvWriter.end();

    convertLogTimesMs.push(convertTimer.stop());

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

  if(USE_TEST_DATES === false) {
    await CsvLogMeta._writeHashMeta(hashLogMeta);
  }

  await destroyWorkers();

  const sumConvertTimeMs = convertLogTimesMs.reduce((acc, curr) => {
    return acc + curr;
  }, 0);
  const avgConvertTimeMs = Math.round(sumConvertTimeMs / convertLogTimesMs.length);
  const [ timeVal, timeLabel ] = getIntuitiveTimeFromMs(avgConvertTimeMs);
  console.log(`\navg convertLog: ${timeVal.toFixed(2)} ${timeLabel}`);
  const sumReadTimeMs = readLogTimesMs.reduce((acc, curr) => {
    return acc + curr;
  }, 0);
  const avgReadTimeMs = Math.round(sumReadTimeMs / readLogTimesMs.length);
  const [ readTimeVal, readTimeLabel ] = getIntuitiveTimeFromMs(avgReadTimeMs);
  console.log(`avg readLog: ${readTimeVal.toFixed(3)} ${readTimeLabel}`);

  console.log(`\nrecordCount: ${recordCount.toLocaleString()}`);
  const recordsWrotePerSecond = Math.round(recordCount / (sumConvertTimeMs / 1000));
  const recordsReadPerSecond = Math.round(recordCount / (sumReadTimeMs / 1000));
  process.stdout.write('\n');
  console.log(`R: ${recordsReadPerSecond.toLocaleString()} records/second`);
  console.log(`W: ${recordsWrotePerSecond.toLocaleString()} records/second`);

  process.stdout.write('\n');
}
