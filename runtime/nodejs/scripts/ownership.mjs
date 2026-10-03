import ts from "typescript";
import { readFileSync, realpathSync } from "node:fs";
import { relative, isAbsolute } from "node:path";
import { builtinModules } from "node:module";

// Explicit application-owned node directories, never inferred from descriptor names.
// Utility imports are traversed too: re-exporting through a helper is not a bypass.
export function checkOwnership(entries, roots) {
  // A nested node owns its subtree regardless of configuration order.
  const owned = roots.map(root => realpathSync(root)).sort((a, b) => b.length - a.length);
  const owner = file => owned.findIndex(root => {
    const path = relative(root, file);
    return path === "" || (!path.startsWith("..") && !isAbsolute(path));
  });
  // Pinned TypeScript's implementation-only resolution excludes declaration
  // files and the package exports "types" condition. Resolve existing runtime
  // files first, then unbuilt TS implementations (e.g. local ./helper.js).
  const options = { module: ts.ModuleKind.NodeNext, moduleResolution: ts.ModuleResolutionKind.NodeNext, allowJs: true, noDtsResolution: true };
  const declaration = file => /\.d\.[cm]?ts$/.test(file);
  const runtimeHost = { ...ts.sys, fileExists: file => !/\.(?:[cm]?ts|tsx)$/.test(file) && ts.sys.fileExists(file) };
  const resolveImport = (file, specifier, mode) => {
    for (const host of [runtimeHost, ts.sys]) {
      const found = ts.resolveModuleName(specifier, file, options, host, undefined, undefined, mode).resolvedModule;
      if (found && !declaration(found.resolvedFileName)) return realpathSync(found.resolvedFileName);
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
      if (declaration(file)) throw new Error("declaration file cannot establish runtime ownership");
      const source = ts.createSourceFile(file, readFileSync(file,"utf8"), ts.ScriptTarget.Latest, true);
      const importMode = ts.getImpliedNodeFormatForFile(file, undefined, ts.sys, options) ?? ts.ModuleKind.ESNext;
      const visit = node => {
        let specifier;
        let mode = importMode;
        if (ts.isImportDeclaration(node)) {
          const clause = node.importClause;
          const bindings = clause?.namedBindings;
          const onlyTypes = clause?.isTypeOnly || (!clause?.name && bindings && ts.isNamedImports(bindings) && bindings.elements.length > 0 && bindings.elements.every(e => e.isTypeOnly));
          if (!onlyTypes) specifier = node.moduleSpecifier;
        }
        if (ts.isExportDeclaration(node) && node.moduleSpecifier) {
          const clause = node.exportClause;
          const onlyTypes = node.isTypeOnly || (clause && ts.isNamedExports(clause) && clause.elements.length > 0 && clause.elements.every(e => e.isTypeOnly));
          if (!onlyTypes) specifier = node.moduleSpecifier;
        }
        if (ts.isImportEqualsDeclaration(node) && !node.isTypeOnly && ts.isExternalModuleReference(node.moduleReference)) {
          specifier = node.moduleReference.expression;
          mode = ts.ModuleKind.CommonJS;
        }
        if (ts.isCallExpression(node) && (node.expression.kind === ts.SyntaxKind.ImportKeyword || node.expression.getText(source) === "require")) {
          if (node.arguments.length !== 1 || !ts.isStringLiteral(node.arguments[0])) throw new Error("dynamic node dependency cannot establish ownership");
          specifier = node.arguments[0];
          mode = node.expression.kind === ts.SyntaxKind.ImportKeyword ? ts.ModuleKind.ESNext : ts.ModuleKind.CommonJS;
        }
        if (specifier) {
          if (!ts.isStringLiteral(specifier)) throw new Error("invalid node dependency");
          if (builtinModules.includes(specifier.text.replace(/^node:/,""))) return;
          visitFile(resolveImport(file,specifier.text,mode));
        }
        ts.forEachChild(node,visit);
      };
      visit(source);
    };
    visitFile(realpathSync(entry));
  }
}
