
export enum DB_COL_TYPES {
  TEXT = 'TEXT',
  NUMERIC = 'NUMERIC',
  INTEGER = 'INTEGER',
  REAL = 'REAL',
  BLOB = 'BLOB',
}
// time_stamp,uri,ping_ms
export const MAIN_TABLE_NAME = 'ping_logs';
export const MAIN_TABLE_COLS: [ string, DB_COL_TYPES, string ][] = [
  [ 'id', DB_COL_TYPES.INTEGER, 'NOT NULL PRIMARY KEY AUTOINCREMENT' ],
  [ 'time_stamp', DB_COL_TYPES.INTEGER, '' ],
  [ 'uri', DB_COL_TYPES.TEXT, '' ],
  [ 'ping_ms', DB_COL_TYPES.NUMERIC, '' ]
];
