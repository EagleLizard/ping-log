
interface TimeBucket {
  recordCount: number;
  pingSum: number;
  successCount: number;
  failCount: number;
}

export class CsvAggregator {
  timeBuckets: Map<string, TimeBucket>
  constructor() {
    this.timeBuckets = new Map;
  }

  aggregate(headers: any[], record: any[]) {
    let id: number, timestamp: number, uri: string, pingMs: number;
    let bucketKey: string, bucket: TimeBucket;
    id = +record[0];
    timestamp = +record[1];
    uri = record[2];
    pingMs = +record[3];
    if(isNaN(timestamp)) {
      console.log(timestamp);
    }
    bucketKey = this.getBucketKey(timestamp, uri);

    if(!this.timeBuckets.has(bucketKey)) {
      this.timeBuckets.set(bucketKey, getBucket());
    }
    bucket = this.timeBuckets.get(bucketKey);
    bucket.recordCount = bucket.recordCount + 1;
    if(isNaN(pingMs)) {
      bucket.failCount++;
    } else {
      bucket.successCount++;
      bucket.pingSum = bucket.pingSum + pingMs;
    }
  }

  getBucketKey(timestamp: number, uri: string) {
    let timeString: string, month: string, day: string, year: number;
    let hours: string, minutes: string, seconds: string;
    let d: Date, key: string;
    d = new Date(timestamp);
    year = d.getFullYear();
    month = `${d.getMonth() + 1}`.padStart(2, '0');
    day = `${d.getDate()}`.padStart(2, '0');
    hours = `${d.getHours()}`.padStart(2, '0');
    minutes = `${d.getMinutes()}`.padStart(2, '0');
    seconds = `${d.getSeconds()}`.padStart(2, '0');
    // timeString = `${hours}:${minutes}:${seconds}`;
    timeString = `${hours}:${minutes}`;
    key = `${year}-${month}-${day}_${timeString}`;
    return key;
  }
}

function getBucket(): TimeBucket {
  return {
    recordCount: 0,
    pingSum: 0,
    successCount: 0,
    failCount: 0,
  };
}
