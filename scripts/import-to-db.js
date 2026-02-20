#!/usr/bin/env node

console.warn(
  "[DEPRECATED] scripts/import-to-db.js -> scripts/pipeline/3-import-to-db.js"
);

const { spawnSync } = require("child_process");
const path = require("path");

const target = path.join(__dirname, "pipeline", "3-import-to-db.js");
const result = spawnSync(process.execPath, [target], { stdio: "inherit" });

if (result.status !== 0) {
  process.exit(result.status || 1);
}
