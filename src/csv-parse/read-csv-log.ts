
import { Timer } from '../lib/timer';
import { scanLog } from '../lib/csv-read';

export interface CsvReadResult {
  readMs: number;
  headers: any[];
  recordCount: number;
}

export async function readCsvLog(logPath: string, recordCb?: (record: any[], idx: number) => void): Promise<CsvReadResult> {
  let headers: any[], recordCount: number, readMs: number;
  let timer: Timer;
  if(!recordCb) {
    recordCb = (record: any[], idx: number) => void 0;
  }

  recordCount = 0;

  timer = Timer.start();
  await scanLog(logPath, (record, idx) => {
    if(idx === 0) {
      headers = record;
    } else {
      recordCount++;
    }
    recordCb(record, idx);
  });
  readMs = timer.stop();

  return {
    readMs,
    headers,
    recordCount,
  };
}
