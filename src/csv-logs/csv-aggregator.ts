
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
    // id = +record[0];
    timestamp = +record[0];
    uri = record[1];
    pingMs = +record[2];

    /*
    ~~~~~~~~~~~~~~~~~~~~~
    */

    if(isNaN(timestamp)) {
      console.log(timestamp);
    }
    bucketKey = this.getBucketKey(timestamp, uri);

    // if(!this.timeBuckets.has(bucketKey)) {
    //   this.timeBuckets.set(bucketKey, getBucket());
    // }
    // bucket = this.timeBuckets.get(bucketKey);
    // bucket.recordCount = bucket.recordCount + 1;

    // if(isNaN(pingMs)) {
    //   bucket.failCount++;
    // } else {
    //   bucket.successCount++;
    //   bucket.pingSum = bucket.pingSum + pingMs;
    // }

    /*
    ~~~~~~~~~~~~~~~~~~~~~
    */

    (
      this.timeBuckets.has(bucketKey)
      || (
        this.timeBuckets.set(bucketKey, getBucket())
      )
    );
    bucket = this.timeBuckets.get(bucketKey);
    bucket.recordCount++;

    (
      isNaN(pingMs) && (
        bucket.failCount++
      )
    );
    (
      !isNaN(pingMs) && (
        bucket.successCount++,
        (bucket.pingSum = bucket.pingSum + pingMs)
      )
    );

  }

  getBucketKey(timestamp: number, uri: string) {
    let timeString: string, month: string, day: string, year: number;
    let minutes: number;
    let hours: string, minutesStr: string, seconds: string;
    let d: Date, key: string;
    d = new Date(timestamp);
    year = d.getFullYear();
    month = `${d.getMonth() + 1}`.padStart(2, '0');
    day = `${d.getDate()}`.padStart(2, '0');
    hours = `${d.getHours()}`.padStart(2, '0');
    minutes = d.getMinutes();
    ((minutes % 15) !== 0)
    && (
      minutes = (minutes - (minutes % 15))
    );
    minutesStr = `${minutes}`.padStart(2, '0');
    seconds = `${d.getSeconds()}`.padStart(2, '0');
    // timeString = `${hours}:${minutes}:${seconds}`;
    timeString = `${hours}:${minutesStr}`;
    key = `${year}-${month}-${day}_${timeString}`;
    return key;
  }

  static merge(aggregators: CsvAggregator[]): CsvAggregator {
    let mergeAggregator: CsvAggregator;
    mergeAggregator = new CsvAggregator;
    for(let i = 0, currAggregator: CsvAggregator; currAggregator = aggregators[i], i < aggregators.length; ++i) {
      let timeBucketTuples: [ string, TimeBucket ][];
      timeBucketTuples = [ ...currAggregator.timeBuckets ];
      for(let k = 0, currTimeBucketTuple: [ string, TimeBucket ]; currTimeBucketTuple = timeBucketTuples[k], k < timeBucketTuples.length; ++k) {
        if(mergeAggregator.timeBuckets.has(currTimeBucketTuple[0])) {
          throw new Error('Conflict in time buckets');
        }
        mergeAggregator.timeBuckets.set(currTimeBucketTuple[0], currTimeBucketTuple[1]);
      }
    }
    return mergeAggregator;
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
