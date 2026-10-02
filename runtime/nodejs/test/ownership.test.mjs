import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
