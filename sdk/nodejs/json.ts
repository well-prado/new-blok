// Lossless JSON boundary: JSON.parse would round int64 values before validation.
export class JSONNumber {
  constructor(readonly text: string) {}
}
export function parseJSON(text: string, maxBytes = 1 << 20): unknown {
  if (Buffer.byteLength(text) > maxBytes) throw new Error("payload_too_large");
  let at = 0;
  const ws = () => { while (/[\t\r\n ]/.test(text[at] ?? "x")) at++; };
  const value = (depth: number): unknown => {
    if (depth > 64) throw new Error("depth_exceeded");
    ws();
    const char = text[at];
    if (char === '"') {
      const start = at++;
      while (at < text.length) {
        if (text[at++] === '"') return JSON.parse(text.slice(start, at)) as string;
        if (text[at - 1] === "\\") at++;
      }
      throw new Error("invalid_value");
    }
    if (char === "{" || char === "[") {
      at++; ws();
      const object: Record<string, unknown> = Object.create(null);
      const array: unknown[] = [];
      const close = char === "{" ? "}" : "]";
      if (text[at] === close) { at++; return char === "{" ? object : array; }
      while (true) {
        if (char === "{") {
          ws(); if (text[at] !== '"') throw new Error("invalid_value");
          const key = value(depth + 1) as string;
          ws(); if (text[at++] !== ":") throw new Error("invalid_value");
          if (Object.hasOwn(object, key)) throw new Error("duplicate_field");
          object[key] = value(depth + 1);
        } else array.push(value(depth + 1));
        ws();
        const next = text[at++];
        if (next === close) return char === "{" ? object : array;
        if (next !== ",") throw new Error("invalid_value");
      }
    }
    for (const [literal, result] of [["true", true], ["false", false], ["null", null]] as const) {
      if (text.startsWith(literal, at)) { at += literal.length; return result; }
    }
    const match = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(text.slice(at));
    if (!match) throw new Error("invalid_value");
    at += match[0].length;
    return new JSONNumber(match[0]);
  };
  const out = value(0); ws();
  if (at !== text.length) throw new Error("trailing_data");
  return out;
}

// Matches encoding/json's sorted map keys and HTML/U+2028/U+2029 escaping.
export function canonicalJSON(value: unknown, depth = 0): string {
  if (depth > 64) throw new Error("depth_exceeded");
  if (value instanceof JSONNumber) return value.text;
  if (typeof value === "bigint") return value.toString();
  if (value === null) return "null";
  if (typeof value === "string") return JSON.stringify(value).replace(/[<>&\u2028\u2029]/g, c => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`);
  if (typeof value === "boolean") return String(value);
  if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(v => canonicalJSON(v, depth + 1)).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) throw new Error("invalid_value");
    return `{${Object.keys(value).sort().map(k => `${canonicalJSON(k)}:${canonicalJSON((value as Record<string, unknown>)[k], depth + 1)}`).join(",")}}`;
  }
  throw new Error("invalid_value");
}
