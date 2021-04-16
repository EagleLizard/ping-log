
import sqlite3, { Database } from 'sqlite3';

import { DB_FILE_NAME } from '../constants';
import {
  MAIN_TABLE_NAME,
  MAIN_TABLE_COLS,
  DB_COL_TYPES,
} from './db-constants';

let pingLogDb: DB;

export class DB {
  private db: Database;
  constructor(db: Database) {
    this.db = db;
  }

  async queryOne(sql: string) {
    return new Promise((resolve, reject) => {
      this.db.get(sql, (err, row) => {
        if(err) {
          return reject(err);
        }
        resolve(row);
      });
    });
  }

  async run(sql: string) {
    return new Promise<void>((resolve, reject) => {
      this.db.run(sql, err => {
        if(err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  async all(sql: string) {
    return new Promise<any[]>((resolve, reject) => {
      this.db.all(sql, (err, rows) => {
        if(err) {
          return reject(err);
        }
        resolve(rows);
      });
    });
  }

  static async initialize() {
    let sqliteDb: Database;
    if(pingLogDb !== undefined) {
      return pingLogDb;
    }
    sqliteDb = new sqlite3.Database(DB_FILE_NAME);
    pingLogDb = new DB(sqliteDb);

    await initializeMainTable(pingLogDb);
    await initializeRecordIdTable(pingLogDb);

    return pingLogDb;
  }

}

async function initializeMainTable(db: DB) {
  let mainTableColStr: string, createQuery: string;

  mainTableColStr = MAIN_TABLE_COLS.map(colTuple => {
    let name: string, colType: DB_COL_TYPES, contraints: string;
    [ name, colType, contraints ] = colTuple;
    return `${name} ${colType} ${contraints}`;
  }).join(',');
  createQuery = `create table if not exists ${MAIN_TABLE_NAME} (${mainTableColStr})`;
  // console.log(createQuery);
  try {
    await db.run(createQuery);
  } catch(e) {
    console.error(createQuery);
    throw e;
  }
}

async function initializeRecordIdTable(db: DB) {
  let tableColStr: string, createQuery: string;
  createQuery = `create table if not exists record_id (id ${DB_COL_TYPES.INTEGER} NOT NULL)`;
  // console.log(createQuery);
  await db.run(createQuery);
}
