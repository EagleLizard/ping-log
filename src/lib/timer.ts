
export class Timer {
  startMs: number;
  endMs: number;
  deltaMs: number;
  private constructor(now: number) {
    this.startMs = now;
  }

  static start(): Timer {
    return new Timer(Date.now());
  }

  stop(): number {
    this.endMs = Date.now();
    this.deltaMs = this.endMs - this.startMs;
    return this.deltaMs;
  }

  current(): number {
    return Date.now() - this.startMs;
  }
}
