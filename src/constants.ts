
import path from 'path';

export const BASE_DIR = path.resolve(__dirname, '../..');
export const HOME_DIR = path.resolve(process.env.HOME);
export const EXT_BASE_DIR = path.resolve('/Volumes/Extreme SSD/dev');

export const PING_MONITOR_DIR = path.join(HOME_DIR, 'repos', 'ping-monitor');

const METADATA_DIR_NAME = 'log-meta';
export const METADATA_DIR_PATH = path.join(BASE_DIR, METADATA_DIR_NAME);
export const METADATA_HASH_FILE_NAME = 'source-log-hash.json';
export const METADATA_LAST_ID_FILE_NAME = 'last-id';
// export const METADATA_HASH_FILE_PATH = path.join(METADATA_DIR_PATH, METADATA_HASH_FILE_NAME);

const CONVERTED_CSV_LOGS_DIR_NAME = 'converted-csv-logs';
export const CONVERTED_CSV_LOGS_DIR_PATH = path.join(BASE_DIR, CONVERTED_CSV_LOGS_DIR_NAME);
export const CONVERTED_CSV_LOGS_EXT_DIR_PATH = path.join(EXT_BASE_DIR, CONVERTED_CSV_LOGS_DIR_NAME);
// export const CSV_PING_LOG_DIR = '/Users/tylor/repos/ping-monitor/csv-logs';
// export const CSV_COALESCE_LOG_DIR = '/Users/tylor/repos/ping-monitor/csv-logs-coalesced';
export const CSV_PING_LOG_DIR = path.join(PING_MONITOR_DIR, 'csv-logs');
export const CSV_COALESCE_LOG_DIR = path.join(PING_MONITOR_DIR, 'csv-logs-coalesced');

export const PERIOD_STAT_PATH = path.join(BASE_DIR, 'stat.txt');

export const DB_FILE_NAME = 'db_ping-log';
