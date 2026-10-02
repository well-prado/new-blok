import ts from "typescript";
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { dirname, resolve, relative, isAbsolute } from "node:path";
import { builtinModules } from "node:module";

// Explicit application-owned node directories, never inferred from descriptor names.
// Utility imports are traversed too: re-exporting through a helper is not a bypass.
export function checkOwnership(entries, roots) {
  const owned = roots.map(root => realpathSync(root));
  const owner = file => owned.findIndex(root => {
    const path = relative(root, file);
    return path === "" || (!path.startsWith("..") && !isAbsolute(path));
  });
  const resolveImport = (file, specifier) => {
    const options = { module: ts.ModuleKind.NodeNext, moduleResolution: ts.ModuleResolutionKind.NodeNext, allowJs: true };
    const found = ts.resolveModuleName(specifier, file, options, ts.sys).resolvedModule;
    if (found) return realpathSync(found.resolvedFileName);
    if (specifier.startsWith(".")) {
      const base = resolve(dirname(file), specifier);
      for (const candidate of [base, base.replace(/\.js$/, ".ts"), `${base}.ts`, resolve(base,"index.ts")]) {
        if (existsSync(candidate)) return realpathSync(candidate);
      }
    }
    throw new Error(`unresolved node dependency: ${specifier}`);
  };
  for (const entry of entries) {
    const origin = owner(realpathSync(entry));
    if (origin < 0) throw new Error("node entry has no declared owner");
    const seen = new Set();
    const visitFile = file => {
      if (seen.has(file)) return;
      seen.add(file);
      const target = owner(file);
      if (target >= 0 && target !== origin) throw new Error("node imports another node");
      if (file.endsWith(".d.ts")) return;
      const source = ts.createSourceFile(file, readFileSync(file,"utf8"), ts.ScriptTarget.Latest, true);
      const visit = node => {
        let specifier;
        if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier) specifier = node.moduleSpecifier;
        if (ts.isCallExpression(node) && (node.expression.kind === ts.SyntaxKind.ImportKeyword || node.expression.getText(source) === "require")) {
          if (node.arguments.length !== 1 || !ts.isStringLiteral(node.arguments[0])) throw new Error("dynamic node dependency cannot establish ownership");
          specifier = node.arguments[0];
        }
        if (specifier) {
          if (!ts.isStringLiteral(specifier)) throw new Error("invalid node dependency");
          if (builtinModules.includes(specifier.text.replace(/^node:/,""))) return;
          visitFile(resolveImport(file,specifier.text));
        }
        ts.forEachChild(node,visit);
      };
      visit(source);
    };
    visitFile(realpathSync(entry));
  }
}
