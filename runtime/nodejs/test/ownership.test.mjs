import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import { checkOwnership } from "../scripts/ownership.mjs";

test("ownership checker traverses helpers, exports and dynamic imports", () => {
  const root = mkdtempSync(join(tmpdir(),"blok-ownership-"));
  try {
    const a=join(root,"a"), b=join(root,"b");mkdirSync(a);mkdirSync(b);
    const entry=join(a,"index.ts");writeFileSync(join(b,"index.ts"),"export const node = 1;");
    writeFileSync(join(root,"helper.ts"),"export * from './b/index.js';");
    for (const source of ["import '../b/index.js';", "export * from '../helper.js';", "const n=import('../b/index.js');", "const n=require('../b/index.js');", "const n=import(variable);"]) {
      writeFileSync(entry,source);assert.throws(()=>checkOwnership([entry],[a,b]));
    }
    writeFileSync(join(a,"helper.ts"),"export const value = 1;");
    writeFileSync(entry,"import './helper.js';");assert.doesNotThrow(()=>checkOwnership([entry],[a,b]));
  } finally { rmSync(root,{recursive:true,force:true}); }
});

function packageFixture(run) {
  const root = mkdtempSync(join(tmpdir(), "blok-ownership-package-"));
  const a = join(root, "a"), b = join(root, "b"), pkg = join(root, "node_modules", "helper");
  for (const dir of [a, b, pkg]) mkdirSync(dir, { recursive: true });
  writeFileSync(join(root, "package.json"), JSON.stringify({ type: "module" }));
  writeFileSync(join(b, "index.js"), "export const value = 7;");
  writeFileSync(join(b, "index.cjs"), "exports.value = 7;");
  writeFileSync(join(pkg, "package.json"), JSON.stringify({ name: "helper", type: "module", exports: { ".": { types: "./index.d.ts", import: "./runtime.js", require: "./runtime.cjs" } } }));
  writeFileSync(join(pkg, "index.d.ts"), "export declare const value: number;");
  const fixture = { a, b, pkg, entry: join(a, "index.ts"), runtimeEntry: join(a, "index.mjs") };
  return Promise.resolve().then(() => run(fixture)).finally(() => rmSync(root, { recursive: true, force: true }));
}

test("package value imports traverse executable exports despite harmless declarations", () => packageFixture(async ({ a, b, pkg, entry, runtimeEntry }) => {
  writeFileSync(join(pkg, "runtime.js"), "export { value } from '../../b/index.js';");
  writeFileSync(join(pkg, "runtime.cjs"), "module.exports = require('../../b/index.cjs');");
  writeFileSync(runtimeEntry, "export { value } from 'helper';");
  assert.equal((await import(pathToFileURL(runtimeEntry).href)).value, 7);
  assert.equal(createRequire(runtimeEntry)("helper").value, 7);
  for (const source of ["import { value } from 'helper';", "export * from 'helper';", "const value = import('helper');", "const value = require('helper');", "import helper = require('helper');"]) {
    writeFileSync(entry, source);
    assert.throws(() => checkOwnership([entry], [a, b]), /node imports another node/);
  }
  const bridge = join(pkg, "..", "bridge");
  mkdirSync(bridge);
  writeFileSync(join(bridge, "package.json"), JSON.stringify({ name: "bridge", type: "module", exports: { types: "./index.d.ts", import: "./index.js", require: "./index.cjs" } }));
  writeFileSync(join(bridge, "index.d.ts"), "export declare const value: number;");
  writeFileSync(join(bridge, "index.js"), "export { value } from '../../b/index.js';");
  writeFileSync(join(bridge, "index.cjs"), "module.exports = require('../../b/index.cjs');");
  writeFileSync(join(pkg, "runtime.js"), "export { value } from 'bridge';");
  writeFileSync(join(pkg, "runtime.cjs"), "module.exports = require('bridge');");
  for (const source of ["import 'helper';", "const value = require('helper');"]) {
    writeFileSync(entry, source);
    assert.throws(() => checkOwnership([entry], [a, b]), /node imports another node/);
  }
}));

test("package pure helpers with separated types and import/require exports pass", () => packageFixture(async ({ a, b, pkg, entry, runtimeEntry }) => {
  writeFileSync(join(pkg, "runtime.js"), "export { value } from './pure.js';");
  writeFileSync(join(pkg, "pure.js"), "export const value = 11;");
  writeFileSync(join(pkg, "runtime.cjs"), "exports.value = 11;");
  writeFileSync(runtimeEntry, "export { value } from 'helper';");
  assert.equal((await import(pathToFileURL(runtimeEntry).href)).value, 11);
  for (const source of ["import { value } from 'helper';", "export * from 'helper';", "const value = import('helper');", "const value = require('helper');"]) {
    writeFileSync(entry, source);
    assert.doesNotThrow(() => checkOwnership([entry], [a, b]));
  }
  // Existing executable JS must win over a sibling TypeScript implementation.
  writeFileSync(entry, "import 'helper';");
  writeFileSync(join(pkg, "pure.ts"), "export { value } from '../../b/index.js';");
  assert.doesNotThrow(() => checkOwnership([entry], [a, b]));
  writeFileSync(join(pkg, "pure.ts"), "export const value = 11;");
  writeFileSync(join(pkg, "pure.js"), "export { value } from '../../b/index.js';");
  assert.throws(() => checkOwnership([entry], [a, b]), /node imports another node/);
}));

test("type-only imports and re-exports do not create executable ownership edges", () => packageFixture(({ a, b, entry }) => {
  for (const source of ["import type { value } from 'helper';", "import { type value } from 'helper';", "export type { value } from '../b/index.js';", "export { type value } from '../b/index.js';", "export type * from 'helper';", "import type helper = require('helper');"]) {
    writeFileSync(entry, source);
    assert.doesNotThrow(() => checkOwnership([entry], [a, b]));
  }
  writeFileSync(entry, "import { type value, missingRuntimeValue } from 'helper';");
  assert.throws(() => checkOwnership([entry], [a, b]), /unresolved node dependency/);
}));

test("value imports fail closed for missing runtime exports and computed dependencies", () => packageFixture(({ a, b, pkg, entry }) => {
  for (const source of ["import 'helper';", "const value = import('helper');", "const value = require('helper');", "import './missing.js';", "import '../node_modules/helper/index.d.ts';"]) {
    writeFileSync(entry, source);
    assert.throws(() => checkOwnership([entry], [a, b]), /unresolved node dependency/);
  }
  writeFileSync(join(pkg, "runtime.js"), "const dependency = 'other'; export const value = import(dependency);");
  writeFileSync(entry, "import 'helper';");
  assert.throws(() => checkOwnership([entry], [a, b]), /dynamic node dependency/);
  writeFileSync(join(pkg, "runtime.js"), "export * from 'missing-package';");
  assert.throws(() => checkOwnership([entry], [a, b]), /unresolved node dependency/);
}));

test("import and require resolve their own executable conditions", () => packageFixture(({ a, b, pkg, entry }) => {
  writeFileSync(join(pkg, "runtime.js"), "export const value = 1;");
  writeFileSync(join(pkg, "runtime.cjs"), "module.exports = require('../../b/index.cjs');");
  writeFileSync(entry, "import 'helper'; const value = import('helper');");
  assert.doesNotThrow(() => checkOwnership([entry], [a, b]));
  writeFileSync(entry, "const value = require('helper');");
  assert.throws(() => checkOwnership([entry], [a, b]), /node imports another node/);
  const cts = join(a, "index.cts");
  writeFileSync(cts, "import { value } from 'helper';");
  assert.throws(() => checkOwnership([cts], [a, b]), /node imports another node/);
}));
