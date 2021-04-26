
import path, { ParsedPath } from 'path';
import { _HashLogMetaValue } from '../csv-logs/csv-log-meta';

const WEEK_AGO_MS = Date.now() - (1000 * 60 * 60 * 24 * 7);
export interface CsvPathDate {
  date: Date;
  dateMs: number;
  dateStr: string;
  csvPaths: string[];
}

export function parseCsvLogFileDate(logFileDate: string): Date {
  let splat: string[], day: number, month: number, year: number;
  let resultDate: Date;
  splat = logFileDate.split('-');
  month = (+splat[0]) - 1;
  day = +splat[1];
  year = +splat[2];
  resultDate = new Date(year, month, day);
  return resultDate;
}

export function getCsvDateMap(csvPaths: string[]): Map<string, string[]> {
  let csvDateMap: Map<string, string[]>;
  csvDateMap = new Map;

  csvPaths.reduce((acc, curr) => {
    let parsedPath: ParsedPath, fileName: string, parsedDate: string;
    let currDates: string[];
    parsedPath = path.parse(curr);
    fileName = parsedPath.name;
    parsedDate = fileName.split('_')[0];

    if(!acc.has(parsedDate)) {
      acc.set(parsedDate, []);
    }

    currDates = acc.get(parsedDate);
    currDates.push(curr);

    return acc;
  }, csvDateMap);

  return csvDateMap;
}

export function getCsvPathDates(csvPaths: string[], _hashLogMeta: _HashLogMetaValue[]): CsvPathDate[] {
  let csvPathDateMap: Map<string, string[]>, csvPathDates: CsvPathDate[];
  let nowDate: Date, todayCsvPathDateIdx: number;
  csvPathDateMap = getCsvDateMap(csvPaths);
  csvPathDates = [ ...csvPathDateMap ].map(csvDateTuple => {
    let parsedDate: Date, dateMs: number, csvPathDate: CsvPathDate;
    parsedDate = parseCsvLogFileDate(csvDateTuple[0]);
    dateMs = parsedDate.valueOf();
    csvPathDate = {
      date: parsedDate,
      dateMs,
      dateStr: csvDateTuple[0],
      csvPaths: csvDateTuple[1],
    };
    return csvPathDate;
  });
  csvPathDates.sort((a, b) => {
    if(a.dateMs > b.dateMs) {
      return 1;
    }
    if(a.dateMs < b.dateMs) {
      return -1;
    }
    return 0;
  });
  // pluck out today as it's likely still being appended
  nowDate = new Date;
  todayCsvPathDateIdx = csvPathDates.findIndex(csvPathDate => {
    return (nowDate.getFullYear() === csvPathDate.date.getFullYear())
      && (nowDate.getMonth() === csvPathDate.date.getMonth())
      && (nowDate.getDate() === csvPathDate.date.getDate())
    ;
  });

  if(todayCsvPathDateIdx !== -1) {
    csvPathDates.splice(todayCsvPathDateIdx, 1);
  }
  /*
    only check limited hashes in the past, unless they aren't in the metadata
  */
  csvPathDates = csvPathDates.filter(csvPathDate => {
    let missingMeta: boolean;
    missingMeta = csvPathDate.csvPaths.some(csvPath => {
      let foundMeta: _HashLogMetaValue;
      foundMeta = _hashLogMeta.find(metaVal => {
        return metaVal.filePath === csvPath;
      });
      return foundMeta === undefined;
    });
    return missingMeta || (csvPathDate.date.valueOf() > WEEK_AGO_MS);
  });
  return csvPathDates;
}
