
import fs, { ReadStream } from 'fs';

import csvTransform, { Handler, Transformer as StreamTransformer } from 'stream-transform';
import csvParse, { Parser } from 'csv-parse';

export type RecordHandler = (record: any, recordIdx: number) => void;

export async function scanLog(csvPath: string, recordCb: RecordHandler) {
  let recordIdx: number;
  recordIdx = 0;
  await parseCsv(csvPath, record => {
    recordCb(record, recordIdx++);
  });
}

export function parseCsv(csvPath: string, recordCb: Handler) {
  return new Promise<void>((resolve, reject) => {
    let csvRs: ReadStream, csvParser: Parser, csvTransformer: StreamTransformer;

    csvRs = fs.createReadStream(csvPath);

    csvRs.on('error', err => {
      return reject(err);
    });

    csvParser = csvParse();
    csvParser.on('error', err => {
      reject(err);
    });
    csvParser.on('end', () => {
      csvRs.destroy();
      resolve();
    });

    csvTransformer = csvTransform(recordCb);

    csvRs.pipe(csvParser).pipe(csvTransformer);
  });
}
