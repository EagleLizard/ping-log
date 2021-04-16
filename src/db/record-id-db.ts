
import { DB } from './db';

interface RecordRow {
  rowid: number;
  id: number;
}

export async function dbTest() {
  let id: number;
  await initRecordDb();
  id = await getCurrentId();
  console.log(id);
  for(let i = 0; i < 10; ++i) {
    const nextId = await incrementRecordId();
    console.log(nextId);
  }
}

export async function initRecordDb() {
  let db: DB, recordRows: any[];
  db = await DB.initialize();
  recordRows = await db.all('SELECT rowid, * FROM record_id');
  console.log(recordRows);
  if(recordRows.length === 0) {
    await db.run('INSERT INTO record_id VALUES (0)');
  }
}

export async function getCurrentId() {
  let db: DB, recordRows: any[], recordRow: RecordRow;
  db = await DB.initialize();
  recordRows = await db.all('SELECT rowid, * FROM record_id');
  recordRow = recordRows[0];
  return recordRow.id;
}

export async function incrementRecordId() {
  let db: DB, recordRows: any[], recordRow: RecordRow;
  db = await DB.initialize();
  recordRows = await db.all('SELECT rowid, * FROM record_id');
  recordRow = recordRows[0];
  recordRow.id = recordRow.id + 1;
  await db.run(`UPDATE record_id SET id = ${recordRow.id} WHERE rowid = ${recordRow.rowid}`);
  return recordRow.id;
}
