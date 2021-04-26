
import { CsvAggregator } from '../csv-logs/csv-aggregator';
import { scanLog } from '../lib/csv-read';

export interface CsvLogParseResult {
  numRecords: number;
  parseMs: number;
  aggregator: CsvAggregator;
}

export async function parseCsvLog(logPath: string): Promise<CsvLogParseResult> {
  let numRecords: number, parseMs: number;
  let startMs: number, endMs: number;
  let headers: string[], aggregator: CsvAggregator;
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
  };
}
