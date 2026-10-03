import type { ServerDuplexStream } from "@grpc/grpc-js";
import type { Frame, ReceivedFrame } from "./protocol.js";

// Includes both our queue and grpc-js callback-pending writes in the budget.
export class BoundedWriter {
  private queue: { frame: Frame; bytes: number }[] = [];
  private count = 0;
  private bytes = 0;
  private blocked = false;
  private closed = false;
  private timer: NodeJS.Timeout | undefined;
  constructor(private readonly stream: Pick<ServerDuplexStream<ReceivedFrame, Frame>, "write" | "on" | "removeListener">,
    private readonly size: (frame: Frame) => number, private readonly fail: () => void,
    private readonly maxCount = 64, private readonly maxBytes = 2 << 20, private readonly stallMs = 5000) {
    stream.on("drain", this.drain);
  }
  private drain = (): void => { this.blocked = false; if (this.timer) clearTimeout(this.timer); this.timer = undefined; this.flush(); };
  send(frame: Frame): void {
    if (this.closed) return;
    const bytes = this.size(frame);
    if (this.count + 1 > this.maxCount || this.bytes + bytes > this.maxBytes) { this.fail(); return; }
    this.count++; this.bytes += bytes; this.queue.push({ frame, bytes }); this.flush();
  }
  private flush(): void {
    while (!this.closed && !this.blocked && this.queue.length) {
      const next = this.queue.shift()!;
      try {
        this.blocked = !this.stream.write(next.frame, (err?: Error | null) => {
          this.count--; this.bytes -= next.bytes;
          if (err && !this.closed) this.fail();
        });
        if (this.blocked) this.timer = setTimeout(() => this.fail(), this.stallMs);
      } catch { this.fail(); return; }
    }
  }
  close(): void {
    this.closed = true; this.queue = [];
    if (this.timer) clearTimeout(this.timer);
    this.stream.removeListener("drain", this.drain);
  }
}
