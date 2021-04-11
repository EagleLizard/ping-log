
import path from 'path';
import { promisify } from 'util';
import fs, { Stats } from 'fs';
const readdir = promisify(fs.readdir);
const stat = promisify(fs.stat);
const mkdir = promisify(fs.mkdir);
const access = promisify(fs.access);

export async function listDir(dirPath: string) {
  let fileNames: string[], filePaths: string[];
  fileNames = await readdir(dirPath);
  filePaths = fileNames.map(fileName => path.join(dirPath, fileName));
  return filePaths;
}

export async function getStats(paths: string[]): Promise<Stats[]> {
  let fileStatPromises: Promise<Stats>[], fileStats: Stats[];
  fileStatPromises = paths.map(path => stat(path));
  fileStats = await Promise.all(fileStatPromises);
  return fileStats;
}

export async function mkdirIfNotExist(dirPath: string) {
  try {
    await mkdir(dirPath);
  } catch(e) {
    if(!(e.code === 'EEXIST')) {
      throw e;
    }
  }
}

export async function exists(filePath: string) {
  let fileExists;
  try {
    await access(filePath, fs.constants.F_OK);
    fileExists = true;
  } catch(e) {
    fileExists = false;
  }
  return fileExists;
}

