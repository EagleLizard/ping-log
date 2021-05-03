
export function sleep(ms: number) {
  return new Promise<void>(resolve => {
    setTimeout(() => {
      resolve();
    }, ms);
  });
}

export function sleepImmediate() {
  return new Promise<void>(resolve => {
    setImmediate(() => {
      resolve();
    });
  });
}
