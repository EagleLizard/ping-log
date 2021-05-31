
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

export const ASYNC_READ_RECORD_QUEUE_SIZE = Math.round(
  // 0.128e3
  // 0.256e3
  // 512
  // 1024
  // 2048
  // 4096
  // 8192

  1.024e4
  // 4.028e4
  // 8.056e4
  // 1.6384e4
  // 1.024e5
);
