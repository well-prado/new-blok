import { canonicalJSON, JSONNumber, parseJSON } from "./json.js";

export interface Schema {
  readonly type?: string;
  readonly properties?: Readonly<Record<string, Schema>>;
  readonly required?: readonly string[];
  readonly items?: Schema;
  readonly anyOf?: readonly Schema[];
  readonly nullable?: boolean;
  readonly additionalProperties?: boolean;
  readonly default?: unknown;
  readonly minimum?: number | bigint | JSONNumber;
  readonly maximum?: number | bigint | JSONNumber;
  readonly wire?: string;
  readonly format?: string;
}
export class SchemaError extends Error {
  constructor(readonly code: string, readonly path = "$") { super(`${code} at ${path}`); }
}
const fail = (code: string, path: string): never => { throw new SchemaError(code, path); };
const object = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v) && !(v instanceof JSONNumber);
const i64min = -(1n << 63n), i64max = (1n << 63n) - 1n;
function bound(value: unknown, path: string): bigint {
  const text = value instanceof JSONNumber ? value.text : String(value);
  if (!/^-?[0-9]+$/.test(text) || (typeof value === "number" && !Number.isSafeInteger(value))) return fail("invalid_schema", path);
  const n = BigInt(text);
  if (n < i64min || n > i64max) return fail("invalid_schema", path);
  return n;
}

export function compileSchema(source: Schema | string): Schema {
  let s: Schema;
  try { s = parseJSON(typeof source === "string" ? source : canonicalJSON(source)) as Schema; }
  catch (e) { throw new SchemaError(e instanceof Error ? e.message : "invalid_schema"); }
  const check = (s: Schema, path: string, depth: number): void => {
    if (depth > 64) fail("depth_exceeded", path);
    if (!object(s)) fail("invalid_schema", path);
    const keys = new Set(["type", "properties", "required", "items", "anyOf", "nullable", "additionalProperties", "default", "minimum", "maximum", "wire", "format"]);
    if (Object.keys(s).some(k => !keys.has(k))) fail("invalid_schema", path);
    if (s.type !== undefined && typeof s.type !== "string") fail("invalid_schema", path);
    for (const k of ["nullable", "additionalProperties"] as const) if (s[k] !== undefined && typeof s[k] !== "boolean") fail("invalid_schema", path);
    for (const k of ["wire", "format"] as const) if (s[k] !== undefined && typeof s[k] !== "string") fail("invalid_schema", path);
    if (s.anyOf !== undefined && !Array.isArray(s.anyOf)) fail("invalid_schema", path);
    if (s.required !== undefined && (!Array.isArray(s.required) || s.required.some(x => typeof x !== "string"))) fail("invalid_schema", path);
    if (s.properties !== undefined && !object(s.properties)) fail("invalid_schema", path);
    if (!s.anyOf?.length && !["string", "boolean", "integer", "number", "object", "array", "null"].includes(s.type ?? "")) fail("unsupported_schema", path);
    const min = s.minimum === undefined ? undefined : bound(s.minimum, path);
    const max = s.maximum === undefined ? undefined : bound(s.maximum, path);
    if (min !== undefined && max !== undefined && min > max) fail("invalid_range", path);
    if (s.type === "integer" && s.wire && s.wire !== "int64-string") fail("unsupported_schema", path);
    if (s.type === "number" && s.wire && s.wire !== "decimal-string") fail("unsupported_schema", path);
    // Validate even inactive children, preventing hidden recursive/unbounded metadata.
    for (const [k, child] of Object.entries(s.properties ?? {})) check(child, `${path}.${k}`, depth + 1);
    for (const k of s.required ?? []) if (!Object.hasOwn(s.properties ?? {}, k)) fail("invalid_schema", path);
    if (s.type === "array" && !s.items) fail("invalid_schema", path);
    if (s.items) check(s.items, `${path}[]`, depth + 1);
    for (const child of s.anyOf ?? []) check(child, `${path}.anyOf`, depth + 1);
    if (Object.hasOwn(s, "default")) {
      try { normalizeAt(s, s.default, path, depth); } catch { fail("invalid_default", path); }
    }
    Object.freeze(s.required); Object.freeze(s.anyOf); Object.freeze(s.properties); Object.freeze(s);
  };
  check(s, "$", 0);
  const freeze = (value: unknown): void => {
    if (typeof value !== "object" || value === null) return;
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  };
  freeze(s);
  return s;
}
function formatValid(value: string, format: string | undefined): boolean {
  if (format === "blob-ref") return value.startsWith("blob://");
  if (format === "byte") {
    const clean = value.replace(/[\r\n]/g, "");
    return /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(clean);
  }
  if (format !== "date-time") return true;
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:[.,]\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
  if (!m) return false;
  const [, year, month, day, hour, minute, second, , tzHour, tzMinute] = m;
  const y = Number(year), mo = Number(month), d = Number(day);
  const days = [31, (y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0)) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return mo >= 1 && mo <= 12 && d >= 1 && d <= days[mo - 1]! && Number(hour) <= 23 && Number(minute) <= 59 && Number(second) <= 59 && Number(tzHour ?? 0) <= 24 && Number(tzMinute ?? 0) <= 60;
}
function normalizeAt(s: Schema, v: unknown, path: string, depth: number): unknown {
  if (depth > 64) return fail("depth_exceeded", path);
  if (v === null) return s.nullable || s.type === "null" ? null : fail("null_not_allowed", path);
  if (s.anyOf?.length) {
    const matches: unknown[] = [];
    for (const candidate of s.anyOf) { try { matches.push(normalizeAt(candidate, v, path, depth + 1)); } catch (e) { if (!(e instanceof SchemaError)) throw e; } }
    return matches.length === 1 ? matches[0] : fail("compatibility_unproven", path);
  }
  switch (s.type) {
    case "object": {
      if (!object(v)) return fail("type_mismatch", path);
      const out: Record<string, unknown> = Object.create(null);
      for (const [k, value] of Object.entries(v)) {
        const child = Object.hasOwn(s.properties ?? {}, k) ? s.properties![k] : undefined;
        if (!child && !s.additionalProperties) return fail("unknown_field", `${path}.${k}`);
        out[k] = child ? normalizeAt(child, value, `${path}.${k}`, depth + 1) : value;
      }
      for (const [k, child] of Object.entries(s.properties ?? {})) if (!Object.hasOwn(v, k) && Object.hasOwn(child, "default")) out[k] = normalizeAt(child, child.default, `${path}.${k}`, depth + 1);
      for (const k of s.required ?? []) if (!Object.hasOwn(out, k)) return fail("missing_required", `${path}.${k}`);
      return out;
    }
    case "array": return Array.isArray(v) ? v.map((x, i) => normalizeAt(s.items!, x, `${path}[${i}]`, depth + 1)) : fail("type_mismatch", path);
    case "string": return typeof v !== "string" ? fail("type_mismatch", path) : formatValid(v, s.format) ? v : fail("invalid_format", path);
    case "boolean": return typeof v === "boolean" ? v : fail("type_mismatch", path);
    case "integer": {
      if (typeof v === "string" && s.wire !== "int64-string") return fail("type_mismatch", path);
      if (!(v instanceof JSONNumber) && !["string", "number", "bigint"].includes(typeof v)) return fail("type_mismatch", path);
      const text = v instanceof JSONNumber ? v.text : String(v);
      if (!/^[+-]?[0-9]+$/.test(text) || (typeof v === "number" && !Number.isSafeInteger(v))) return fail("integer_range", path);
      const n = BigInt(text);
      if (n < i64min || n > i64max || (s.minimum !== undefined && n < bound(s.minimum, path)) || (s.maximum !== undefined && n > bound(s.maximum, path))) return fail("integer_range", path);
      return s.wire === "int64-string" ? n.toString() : n >= BigInt(Number.MIN_SAFE_INTEGER) && n <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(n) : n;
    }
    case "number": {
      if (s.wire === "decimal-string") return typeof v === "string" && /^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(v) ? v : fail("decimal_invalid", path);
      if (!(v instanceof JSONNumber) && typeof v !== "number") return fail("type_mismatch", path);
      return Number.isFinite(Number(v instanceof JSONNumber ? v.text : v)) ? v : fail("number_invalid", path);
    }
    // Go's current subset normalizes non-null values under a null schema to null.
    case "null": return null;
    default: return fail("unsupported_schema", path);
  }
}
export function normalize(s: Schema, value: unknown): unknown {
  // Copy and bound all values, including unknown fields allowed by a schema.
  const copied = parseJSON(canonicalJSON(value));
  return normalizeAt(s, copied, "$", 0);
}
export function normalizeJSON(s: Schema, text: string): string {
  let v: unknown;
  try { v = parseJSON(text); } catch (e) { throw new SchemaError(e instanceof Error ? e.message : "invalid_value"); }
  return canonicalJSON(normalizeAt(s, v, "$", 0));
}
