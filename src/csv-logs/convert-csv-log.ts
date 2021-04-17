import os from 'os';

import _chunk from 'lodash.chunk';

import { CONVERTED_CSV_LOGS_DIR_PATH } from '../constants';
import { scanLog } from '../lib/csv-read';
import { writeCsv, CsvWriter } from '../lib/csv-writer';
import { CsvPathDate } from '../lib/date-time-util';
import { CsvLogMeta } from './csv-log-meta';
import { getFileHash } from './hash-log';
import { getRecordId } from './record-id';
import { incrementRecordId } from '../db/record-id-db';
import { sleep } from '../lib/sleep';

export interface CsvConvertResult {
  csvPathDate: CsvPathDate;
  recordCount: number
  fileHashTuples: [ string, string ][];
}

const NUM_CPUS = os.cpus().length;
const CSV_CHUNK_SIZE = Math.round(
  // 1
  // NUM_CPUS - 1
  NUM_CPUS
  // NUM_CPUS * 4
  // NUM_CPUS * Math.E
  // NUM_CPUS * Math.LOG2E
  // NUM_CPUS / Math.E
  // NUM_CPUS / Math.LOG2E
  // NUM_CPUS / 4
);

export async function converCsvLog(csvPathDate: CsvPathDate): Promise<CsvConvertResult> {
  let csvPaths: string[], dateRecords: any[][], convertedDateRecords: any[][];
  let csvConvertFileName: string, csvConvertFilePath: string;
  let fileHashTuples: [ string, string ][];
  let headers: string[];
  let recordCount: number, recordIdCounter: number;
  let csvFileChunks: string[][];
  let csvWriter: CsvWriter;

  headers = [ 'id', 'time_stamp', 'uri', 'ping_ms' ];
  csvPaths = csvPathDate.csvPaths;
  csvConvertFileName = `${csvPathDate.dateStr}.csv`;
  csvConvertFilePath = `${CONVERTED_CSV_LOGS_DIR_PATH}/${csvConvertFileName}`;

  fileHashTuples = [];
  dateRecords = [];
  convertedDateRecords = [];
  recordCount = 0;
  csvFileChunks = _chunk(csvPathDate.csvPaths, CSV_CHUNK_SIZE);
  for(let i = 0, currCsvFileChunk: string[]; currCsvFileChunk = csvFileChunks[i], i < csvFileChunks.length; ++i) {
    let scanPromises: Promise<void>[];
    scanPromises = [];
    // console.log(currCsvFileChunk);
    for(let k = 0, currCsvPath: string; currCsvPath = currCsvFileChunk[k], k < currCsvFileChunk.length; ++k) {
      let scanPromise: Promise<void>;
      scanPromise = scanLog(currCsvPath, (record, recordIdx) => {
        if((recordIdx === 0) && (headers === undefined)) {
          if(
            record[0] !== 'time_stamp'
            || record[1] !== 'uri'
            || record[2] !== 'ping_ms'
          ) {
            throw new Error(`Unexpected headers from source csv: ${record.join(', ')}`);
          }
        } else if(recordIdx !== 0) {
          recordCount++;
          dateRecords.push(record);
        }
      }).then(() => {
        return getFileHash(currCsvPath).then(hash => {
          fileHashTuples.push([
            currCsvPath,
            hash,
          ]);
        });
      });
      scanPromises.push(scanPromise);
    }
    await Promise.all(scanPromises);
  }

  dateRecords.reverse();

  recordIdCounter = 0;

  while(dateRecords.length) {
    let recordId: number;
    let dateRecord: any[], recordWithId: any[], convertedRecord: any[];
    dateRecord = dateRecords.pop();
    recordId = ++recordIdCounter;
    recordWithId = [ recordId, ...dateRecord ];
    convertedRecord = convertRecord(recordWithId);
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
  csvWriter = new CsvWriter(csvConvertFilePath);
  const convertedCsvRows = [
    headers,
    ...convertedDateRecords,
  ];
  for(let i = 0; i < convertedCsvRows.length; ++ i) {
    csvWriter.write(convertedCsvRows[i]);
  }
  await sleep(1);
  await csvWriter.end();
  // await writeCsv(csvConvertFilePath, [
  //   headers,
  //   ...convertedDateRecords,
  // ]);

  return {
    csvPathDate,
    recordCount,
    fileHashTuples,
  };

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
