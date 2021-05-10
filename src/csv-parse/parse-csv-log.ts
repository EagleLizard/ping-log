
import { CsvAggregator } from '../csv-logs/csv-aggregator';
import { scanLog } from '../lib/csv-read';

export interface CsvLogParseResult {
  numRecords: number;
  parseMs: number;
  aggregator: CsvAggregator;
  headers: any[];
}

export async function parseCsvLog(logPath: string, recordCb?: (record: any[]) => void): Promise<CsvLogParseResult> {
  let numRecords: number, parseMs: number;
  let startMs: number, endMs: number;
  let headers: string[], aggregator: CsvAggregator;
  if(recordCb === undefined) {
    recordCb = (record: any[]) => void 0;
  }

  numRecords = 0;
  aggregator = new CsvAggregator();

  startMs = Date.now();
  await scanLog(logPath, (record, recordIdx) => {
    (
      (recordIdx === 0)
      && (
        headers = record
      )
      || (
        aggregator.aggregate(headers, record),
        // recordCb(record),
        numRecords++
      )
    );
    // if(recordIdx === 0) {
    //   headers = record;
    // } else {
    //   aggregator.aggregate(headers, record);
    //   numRecords++;
    // }
  });
  endMs = Date.now();
  parseMs = endMs - startMs;

  // console.log(headers);
  return {
    numRecords,
    parseMs,
    aggregator,
    headers,
  };
}
