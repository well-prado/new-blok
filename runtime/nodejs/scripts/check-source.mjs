import ts from "typescript";
import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
const root = resolve(import.meta.dirname, "../../..");
const walk = dir => readdirSync(dir, { withFileTypes: true }).flatMap(e => ["node_modules", "dist", "generated"].includes(e.name) ? [] : e.isDirectory() ? walk(resolve(dir, e.name)) : e.name.endsWith(".ts") ? [resolve(dir, e.name)] : []);
for (const file of [...walk(resolve(root, "sdk/nodejs")), ...walk(resolve(root, "runtime/nodejs"))]) {
  const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
  const visit = node => {
    if (node.kind === ts.SyntaxKind.AnyKeyword) throw new Error(`${file}: explicit any prohibited`);
    if (ts.isCallExpression(node) && ["eval", "Function"].includes(node.expression.getText(source))) throw new Error(`${file}: source expressions prohibited`);
    ts.forEachChild(node, visit);
  };
  visit(source);
}
