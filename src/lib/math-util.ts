
export function scaleTo(n: number, fromRange: [ number, number ], toRange: [ number, number ]) {
  let fromMin: number, fromMax: number, toMin: number, toMax: number;
  let scaled: number;
  [ fromMin, fromMax ] = fromRange;
  [ toMin, toMax ] = toRange;
  scaled = (((toMax - toMin) * (n - fromMin)) / (fromMax - fromMin)) + toMin;
  return scaled;
}

export function getLogFn(fromRange: [ number, number ], toRange: [ number, number ]) {
  let baseFn: (n: number) => number, logFromRange: [ number, number ];
  baseFn = Math.log;
  if(fromRange[0] < 1) {
    fromRange[0] = 1;
  }
  logFromRange = [
    baseFn(fromRange[0]),
    baseFn(fromRange[1]),
  ];
  return (n: number) => {
    let logN;
    if(n < 1) {
      n = 1;
    }
    logN = baseFn(n);
    return scaleTo(logN, logFromRange, toRange);
  };
}
