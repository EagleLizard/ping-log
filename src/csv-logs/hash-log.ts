
import { createHash } from 'crypto';
import os from 'os';
import fs, { ReadStream } from 'fs';

import _chunk from 'lodash.chunk';

import { CsvPathDate } from '../lib/date-time-util';
import { printProgress } from '../print';
import { _HashLogMetaValue } from './csv-log-meta';
import { queueHashJob, initializePool } from '../worker-pool/worker-pool';
import { sleep } from '../lib/sleep';

export interface HashLogResult {
  filePath: string;
  fileHash: string;
}

const NUM_CPUS = os.cpus().length;
const CSV_FILES_CHUNK_SIZE = Math.round(
  // 1
  // NUM_CPUS / 3
  NUM_CPUS / Math.E
  // NUM_CPUS / Math.LOG2E
);

export async function getFileHash(filePath: string) {
  return new Promise<string>((resolve, reject) => {
    let readStream: ReadStream;
    const hasher = createHash('md5');

    readStream = fs.createReadStream(filePath);

    readStream.on('data', data => {
      hasher.update(data.toString());
    });

    readStream.on('error', err => {
      reject(err);
    });

    readStream.on('end', () => {
      let hashStr: string;
      hashStr = hasher.digest('base64');
      readStream.destroy();
      resolve(hashStr);
    });

  });
}

export async function getHashesConcurrent(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[]): Promise<[ string, string ][]> {
  let fileHashes: [ string, string ][], totalPathsCount: number, pathsCompletCount: number;
  let startMs: number, endMs: number, deltaMs: number;
  let csvFilePaths: string[], hashJobPromises: Promise<void>[];
  // totalPathsCount = csvPathDates.reduce((acc, curr) => {
  //   return acc + curr.csvPaths.length;
  // }, 0);
  pathsCompletCount = 0;
  fileHashes = [];
  // hashJobPromises = [];
  await initializePool();
  startMs = Date.now();
  csvFilePaths = csvPathDates.reduce((acc, curr) => {
    return acc.concat(curr.csvPaths);
  }, []);
  hashJobPromises = csvFilePaths.map(csvFilePath => {
    return queueHashJob(csvFilePath)
      .then(hashResult => {
        fileHashes.push([
          hashResult.filePath,
          hashResult.fileHash,
        ]);
        pathsCompletCount++;
        printProgress(pathsCompletCount, csvFilePaths.length);
      });
  });
  await Promise.all(hashJobPromises);
  endMs = Date.now();
  deltaMs = endMs - startMs;
  console.log(`\nHash generation took ${deltaMs}ms\n`);
  return fileHashes;
}
