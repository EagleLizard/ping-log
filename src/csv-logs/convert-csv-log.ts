import { CONVERTED_CSV_LOGS_DIR_PATH } from '../constants';
import { scanLog } from '../lib/csv-read';
import { writeCsv } from '../lib/csv-writer';
import { CsvPathDate } from '../lib/date-time-util';
import { CsvLogMeta } from './csv-log-meta';
import { getFileHash } from './hash-log';
import { getRecordId } from './record-id';
import { incrementRecordId } from '../db/record-id-db';

export interface CsvConvertResult {
  csvPathDate: CsvPathDate;
  recordCount: number
  fileHashTuples: [ string, string ][];
}

export async function converCsvLog(csvPathDate: CsvPathDate): Promise<CsvConvertResult> {
  let csvPaths: string[], dateRecords: any[][], convertedDateRecords: any[][];
  let csvConvertFileName: string, csvConvertFilePath: string;
  let fileHashTuples: [ string, string ][];
  let headers: string[];
  let recordCount: number, recordIdCounter: number;

  headers = [ 'id', 'time_stamp', 'uri', 'ping_ms' ];
  csvPaths = csvPathDate.csvPaths;
  csvConvertFileName = `${csvPathDate.dateStr}.csv`;
  csvConvertFilePath = `${CONVERTED_CSV_LOGS_DIR_PATH}/${csvConvertFileName}`;

  fileHashTuples = [];
  dateRecords = [];
  convertedDateRecords = [];
  recordCount = 0;

  for(let i = 0, currCsvPath: string; currCsvPath = csvPaths[i], i < csvPaths.length; ++i) {
    await scanLog(currCsvPath, (record, recordIdx) => {
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

  await writeCsv(csvConvertFilePath, [
    headers,
    ...convertedDateRecords,
  ]);

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
