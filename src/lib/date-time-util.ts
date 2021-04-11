
import path, { ParsedPath } from 'path';

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
