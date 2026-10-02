import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { checkOwnership } from "./ownership.mjs";
const configPath=resolve(process.argv[2] ?? "ownership.json");
const config=JSON.parse(readFileSync(configPath,"utf8"));
if (!Array.isArray(config.entries) || !Array.isArray(config.roots) || !config.entries.length || !config.roots.length || [...config.entries,...config.roots].some(x=>typeof x!=="string")) throw new Error("invalid ownership configuration");
checkOwnership(config.entries.map(x=>resolve(dirname(configPath),x)),config.roots.map(x=>resolve(dirname(configPath),x)));
