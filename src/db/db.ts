
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

  static async initialize() {
    let sqliteDb: Database, mainTableColStr: string, createQuery: string;
    if(pingLogDb !== undefined) {
      return pingLogDb;
    }
    sqliteDb = new sqlite3.Database(DB_FILE_NAME);
    pingLogDb = new DB(sqliteDb);

    mainTableColStr = MAIN_TABLE_COLS.map(colTuple => {
      let name: string, colType: DB_COL_TYPES, contraints: string;
      [ name, colType, contraints ] = colTuple;
      return `${name} ${colType} ${contraints}`;
    }).join(',');
    createQuery = `create table if not exists ${MAIN_TABLE_NAME} (${mainTableColStr})`;
    try {
      await pingLogDb.run(createQuery);
    } catch(e) {
      console.error(createQuery);
      throw e;
    }
    return pingLogDb;
  }
}
