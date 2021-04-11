
import path from 'path';
import fs from 'fs';
import { Writable } from 'stream';

import csvStringify, { Stringifier } from 'csv-stringify';

export class CsvWriter {
  csvPath: string;
  private csvWriteStream: Writable;
  private csvStringifier: Stringifier;

  constructor(csvPath: string) {
    this.csvPath = csvPath;
    this.csvWriteStream = fs.createWriteStream(csvPath);
    this.csvStringifier = csvStringify();
    this.csvStringifier.pipe(this.csvWriteStream);
  }

  write(record: any[]) {
    try {
      this.csvStringifier.write(record);
    } catch(e) {
      console.error(e);
      throw e;
    }
  }

  end(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.csvWriteStream.on('close', () => {
        this.csvWriteStream.destroy();
        resolve();
      });
      this.csvStringifier.end();
    });
  }
}

export function writeCsv(filePath: string, records: any[][]): Promise<void> {
  return new Promise((resolve, reject) => {
    csvStringify(records, (err, output) => {
      if(err) {
        return reject(err);
      }
      fs.writeFile(filePath, output, err => {
        if(err) {
          return reject(err);
        }
        resolve();
      });
    });
  });
}
