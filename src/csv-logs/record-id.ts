import { CsvLogMeta } from './csv-log-meta';

let recordIdCounter: number;
export function getRecordId() {
  let nextId: number;
  if(recordIdCounter === undefined) {
    throw new Error('recordIdCounter is undefined');
  }
  nextId = ++recordIdCounter;
  if(recordIdCounter > Number.MAX_SAFE_INTEGER) {
    throw new Error(`Record ID integer exceeded MAX_SAFE_INTEGER: ${recordIdCounter}`);
  }
  return nextId;
}

export async function initializeRecordId() {
  recordIdCounter = await CsvLogMeta.getLastId();
  return recordIdCounter;
}
