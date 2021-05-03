
import path from 'path';
import fs from 'fs';
import { Writable } from 'stream';

import csvStringify, { Stringifier } from 'csv-stringify';
import { sleep, sleepImmediate } from './sleep';

export class CsvWriter {
  csvPath: string;
  private csvWriteStream: Writable;
  private csvStringifier: Stringifier;
  private draining: boolean;

  constructor(csvPath: string) {
    this.csvPath = csvPath;
    this.draining = false;
    this.csvWriteStream = fs.createWriteStream(csvPath);
    this.csvStringifier = csvStringify();
    this.csvStringifier.pipe(this.csvWriteStream);
  }

  async write(record: any[]) {
    while(this.draining) {
      // console.log('drain');
      // await sleep(0);
      await sleepImmediate();
    }
    try {
      return new Promise<void>(resolve => {
        if(!this.csvStringifier.write(record)) {
          this.draining = true;
          this.csvStringifier.once('drain', () => {
            this.draining = false;
            resolve();
          });
        } else {
          resolve();
        }
      });
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
