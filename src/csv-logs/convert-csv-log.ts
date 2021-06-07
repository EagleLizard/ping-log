
import { scanLog } from '../lib/csv-read';

export interface CsvLogConvertResult {
  filePath: string;
  headers: any[];
  records: any[][];
  recordCount: number;
}

export async function convertCsvLogFile(
  filePath: string,
  recordCb?: (record: any[]) => void
): Promise<CsvLogConvertResult> {
  let headers: any[], records: any[][];
  let recordCount: number;
  if(recordCb === undefined) {
    recordCb = (record: any[]) => void 0;
  }
  records = [];
  recordCount = 0;

  await scanLog(filePath, (record, recordIdx) => {
    let convertedRecord: any[];
    if(
      (recordIdx === 0)
      && (
        (record[0] !== 'time_stamp')
          || (record[1] !== 'uri')
          || (record[2] !== 'ping_ms')
      )
    ) {
      throw new Error(`Unexpected headers from source csv: ${record.join(', ')}`);
    } else {
      if(recordIdx !== 0) {
        recordCount++;
        convertedRecord = [
          (new Date(record[0])).valueOf(),
          record[1],
          +record[2],
        ];
        recordCb(convertedRecord);
      }
    }
  });

  return {
    filePath,
    headers,
    records,
    recordCount,
  };
}
