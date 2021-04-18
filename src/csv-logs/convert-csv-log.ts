
import { scanLog } from '../lib/csv-read';
import { getFileHash } from './hash-log';

export interface CsvLogConvertResult {
  filePath: string;
  fileHash: string;
  headers: any[];
  records: any[][];
  recordCount: number;
}

export async function convertCsvLogFile(filePath: string): Promise<CsvLogConvertResult> {
  let headers: any[], records: any[][], fileHash: string;
  let recordCount: number;
  records = [];
  recordCount = 0;
  await scanLog(filePath, (record, recordIdx) => {
    if(recordIdx === 0) {
      if(
        record[0] !== 'time_stamp'
        || record[1] !== 'uri'
        || record[2] !== 'ping_ms'
      ) {
        throw new Error(`Unexpected headers from source csv: ${record.join(', ')}`);
      }
      headers = record;
    } else if(recordIdx !== 0) {
      recordCount++;
      records.push(record);
    }
  });
  fileHash = await getFileHash(filePath);

  return {
    filePath,
    fileHash,
    headers,
    records,
    recordCount,
  };
}
