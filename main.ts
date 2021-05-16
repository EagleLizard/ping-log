import sourceMapSupport from 'source-map-support';
sourceMapSupport.install();

import { convertCsvLogs } from './src/csv-logs/convert-csv-logs';
import { _parseCsvLogs } from './src/csv-parse/_parse-csv-logs';
import { mergeCsvLogs } from './src/csv-parse/merge-csv-logs';
import { dbTest } from './src/db/record-id-db';

(async () => {
  try {
    await main(process.argv);
  } catch(e) {
    console.error(e);
    throw e;
  }
})();

async function main(argv: string[]) {
  const flag = argv[2];

  if(flag === '-c') {
    await convertCsvLogs();
  } else if(flag === '-db') {
    await dbTest();
  } else if(flag === '-m') {
    await mergeCsvLogs();
  } else {
    await _parseCsvLogs();
  }

  printStats();
}

function printStats() {
  let heapTotalMb: number, externalMb: number, totalMb: number;
  heapTotalMb = Math.round(process.memoryUsage().heapTotal / 1024 / 1024);
  externalMb = Math.round(process.memoryUsage().external / 1024 / 1024);
  totalMb = heapTotalMb + externalMb;
  console.log(`Process used ${heapTotalMb}mb of heap memory`);
  console.log(`Process used ${externalMb}mb of external memory`);
  console.log(`Process used ${totalMb}mb of total memory`);
}
