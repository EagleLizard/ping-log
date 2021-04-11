
import path, { ParsedPath } from 'path';
import { promisify } from 'util';
import fs from 'fs';
const readFile = promisify(fs.readFile);
const writeFile = promisify(fs.writeFile);

import {
  CONVERTED_CSV_LOGS_DIR_PATH,
  METADATA_DIR_PATH,
  METADATA_HASH_FILE_NAME,
  METADATA_LAST_ID_FILE_NAME,
} from '../constants';
import { exists, mkdirIfNotExist } from '../lib/files';
import { CsvPathDate } from '../lib/date-time-util';

const METADATA_HASH_FILE_PATH = path.join(METADATA_DIR_PATH, METADATA_HASH_FILE_NAME);
const METADATA_LAST_ID_FILE_PATH = path.join(METADATA_DIR_PATH, METADATA_LAST_ID_FILE_NAME);
export interface _HashLogMetaValue {
  timestamp: number;
  fileKey: string;
  fileHash: string;
  filePath: string;
}

export class CsvLogMeta {
  static async getLogHashMeta() {
    let rawHashMetaData: string, hashLogMeta: _HashLogMetaValue[];
    let _hashLogMeta: _HashLogMetaValue[];
    await initLogMeta();
    rawHashMetaData = (await readFile(METADATA_HASH_FILE_PATH)).toString();
    hashLogMeta = JSON.parse(rawHashMetaData) as _HashLogMetaValue[];
    if(Array.isArray(hashLogMeta)) {
      _hashLogMeta = hashLogMeta as _HashLogMetaValue[];
    } else {
      console.log(typeof hashLogMeta);
      throw new Error('Unexpected hash log meta schema');
    }
    for(let i = 0, currMetaVal: _HashLogMetaValue; currMetaVal = _hashLogMeta[i], i < _hashLogMeta.length; ++i) {
      let metaTimeStamp: Date;
      if(currMetaVal.timestamp === undefined) {
        metaTimeStamp = CsvLogMeta.getTimestampFromFilepath(currMetaVal.filePath);
        currMetaVal.timestamp = metaTimeStamp.valueOf();
      }
    }
    return _hashLogMeta;
  }

  static async _writeHashMeta(hashLogMeta: _HashLogMetaValue[]) {
    let jsonHashLogMeta: string;
    hashLogMeta = hashLogMeta.slice();
    hashLogMeta.sort((a, b) => {
      let aStamp: number, bStamp: number;
      aStamp = a.timestamp;
      bStamp = b.timestamp;
      if(aStamp > bStamp) {
        return 1;
      }
      if(aStamp < bStamp) {
        return -1;
      }
      return 0;
    });
    jsonHashLogMeta = JSON.stringify(hashLogMeta, null, 2);
    await writeFile(METADATA_HASH_FILE_PATH, jsonHashLogMeta);
  }

  static async initConvertedLogsDir() {
    await mkdirIfNotExist(CONVERTED_CSV_LOGS_DIR_PATH);
  }

  static filterHashedCsvPathDates(csvPathDates: CsvPathDate[], hashLogMeta: _HashLogMetaValue[], fileHashTuples: [ string, string ][]): CsvPathDate[] {
    return csvPathDates.map(csvPathDate => {
      let filteredPaths: string[];
      filteredPaths = csvPathDate.csvPaths.filter(csvPath => {
        let foundMetaVal: _HashLogMetaValue;
        const hashMetaKey = path.parse(csvPath).base;
        foundMetaVal = hashLogMeta.find(metaVal => {
          return metaVal.fileKey === hashMetaKey;
        });
        if(foundMetaVal !== undefined) {
          const foundMatchingHashTuple = fileHashTuples.find(hashTuple => {
            if(
              (csvPath === foundMetaVal.filePath)
              && (foundMetaVal.fileHash === hashTuple[1])
            ) {
              return true;
            }
          });
          if(foundMatchingHashTuple !== undefined) {
            return false;
          }
        }
        return true;
      });
      csvPathDate.csvPaths = filteredPaths;
      return csvPathDate;
    });
  }

  static async writeLastId(lastId: number) {
    let lastIdStr: string;
    lastIdStr = `${lastId}`;
    await writeFile(METADATA_LAST_ID_FILE_PATH, lastIdStr);
  }

  static async getLastId(): Promise<number> {
    let lastIdExists: boolean;
    let lastIdFileData: string, lastId: number;
    lastIdExists = await exists(METADATA_LAST_ID_FILE_PATH);
    if(!lastIdExists) {
      return 0;
    }
    lastIdFileData = (await readFile(METADATA_LAST_ID_FILE_PATH))
      .toString()
      .trim();
    lastId = +lastIdFileData;
    if(isNaN(lastId)) {
      throw new Error(`Invalid lastId from file: ${lastIdFileData}`);
    }
    return lastId;
  }

  static getTimestampFromFilepath(filePath: string): Date {
    let parsedPath: ParsedPath, fileName: string, splatName: string[],
      datePart: string, timePart: string, splatDate: string[], splatTime: string[];
    let year: number, monthIdx: number, day: number, hours: number, minutes: number;
    let fileDate: Date;
    parsedPath = path.parse(filePath);
    fileName = parsedPath.name;
    splatName = fileName.split('_');
    datePart = splatName[0];
    timePart = splatName[1];
    splatDate = datePart.split('-');
    splatTime = timePart.split(':');
    monthIdx = (+splatDate[0]) - 1;
    day = +splatDate[1];
    year = +splatDate[2];
    hours = +splatTime[0];
    minutes = +splatTime[1];
    fileDate = new Date(year, monthIdx, day, hours, minutes, 0, 0);
    return fileDate;
  }
}

/*
  check log dir and log meta existence.
    if not exist, initialize.
*/
async function initLogMeta() {
  await mkdirIfNotExist(METADATA_DIR_PATH);
  await initHashLogMeta();
}

async function initHashLogMeta() {
  let fileHashMetaExists: boolean;
  let hashLogMeta: _HashLogMetaValue[];
  fileHashMetaExists = await exists(METADATA_HASH_FILE_PATH);
  if(!fileHashMetaExists) {
    hashLogMeta = [];
    await writeFile(METADATA_HASH_FILE_PATH, JSON.stringify(hashLogMeta));
  }
}
