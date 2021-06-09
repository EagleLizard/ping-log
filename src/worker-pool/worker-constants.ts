
import os from 'os';

export enum MESSAGE_TYPES {
  ACK = 'ack',
  EXIT = 'exit',
  HASH_FILE = 'hash_file',
  HASH_FILE_COMPLETE = 'hash_file_complete',
  PARSE_CSV = 'parse_csv',
  PARSE_CSV_COMPLETE = 'parse_csv_complete',
  CONVERT_CSV = 'convert_csv',
  CONVERT_CSV_COMPLETE = 'convert_csv_complete',
  CONVERT_CSV_LOG = 'convert_csv_log',
  CONVERT_CSV_LOG_COMPLETE = 'convert_csv_log_complete',
  CONVERT_CSV_LOG_RECORD_READ = 'convert_csv_log_record_read',
  CSV_WRITER = 'csv_writer',
  CSV_WRITER_WRITE = 'csv_writer_write',
  CSV_WRITER_WRITE_DONE = 'csv_writer_write_done',
  CSV_WRITER_END = 'csv_writer_end',
  CSV_WRITE = 'csv_write',
  CSV_WRITE_COMPLETE = 'csv_write_complete',
  CSV_READ = 'csv_read',
  CSV_READ_COMPLETE = 'csv_read_complete',
  ASYNC_CSV_READ = 'async_csv_read',
}

const NUM_CPUS = os.cpus().length;
export const NUM_WORKERS = Math.round(
  // 1
  // 2
  // 3
  // NUM_CPUS - 2
  // NUM_CPUS - 1
  // NUM_CPUS
  NUM_CPUS / Math.LOG2E
  // NUM_CPUS / 2
  // NUM_CPUS / 4
  // NUM_CPUS * 2
  // NUM_CPUS * 4
  // NUM_CPUS * 8
  // NUM_CPUS - 2
);

export const MAX_CSV_WRITERS = Math.floor(
  // 1
  // 2
  // 3
  // NUM_WORKERS / 2
  // NUM_WORKERS / 1.5
  // NUM_WORKERS / Math.LOG2E
  // NUM_WORKERS - 1
  NUM_WORKERS
);

export const MAX_CSV_READERS = Math.floor(
  // NUM_WORKERS / 2
  NUM_WORKERS / Math.LOG2E
);
