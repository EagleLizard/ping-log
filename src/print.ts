import { scaleTo } from './lib/math-util';

export function printProgress(completedCount: number, total: number) {
  let progressBar: string, doOverwrite: boolean, prefix: string, postfix: string,
    toWrite: string;
  doOverwrite = (completedCount < total);
  prefix = doOverwrite ? '  ' : '';
  // postfix = doOverwrite ? '\r' : '       \n';
  postfix = ` ${((completedCount / total) * 100).toFixed(3)}%`;
  progressBar = getProgressBar(completedCount, total);
  toWrite = `${prefix}${progressBar}${postfix}`;
  process.stdout.clearLine(undefined);  // clear current text
  process.stdout.cursorTo(0);
  process.stdout.write(toWrite);
  process.stdout.cursorTo(0);
}

function getProgressBar(completedCount: number, total: number) {
  let scaledCompleted: number, scaledDiff: number, progressBar: string;
  scaledCompleted = Math.ceil(
    scaleTo(completedCount, [ 0, total ], [ 0, 100 ])
  );
  scaledDiff = 100 - scaledCompleted;
  progressBar = `[${'-'.repeat(scaledCompleted)}${' '.repeat(scaledDiff)}]`;
  return progressBar;
}

export function getIntuitiveTimeFromMs(ms: number): [ number, string ] {
  let seconds: number, minutes: number;
  let label: string;
  if(ms < (1000)) {
    label = 'ms';
    return [ ms, label ];
  }
  seconds = ms / 1000;
  if(seconds < (60)) {
    label = 'seconds';
    return [ seconds, label ];
  }
  minutes = seconds / 60;
  label = 'minutes';
  return [ minutes, label ];
}