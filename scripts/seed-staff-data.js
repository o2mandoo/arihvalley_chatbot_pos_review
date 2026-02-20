#!/usr/bin/env node

console.warn(
  "[DEPRECATED] scripts/seed-staff-data.js -> scripts/pipeline/4-seed-staff-data.js"
);

const { spawnSync } = require("child_process");
const path = require("path");

const target = path.join(__dirname, "pipeline", "4-seed-staff-data.js");
const result = spawnSync(process.execPath, [target], { stdio: "inherit" });

if (result.status !== 0) {
  process.exit(result.status || 1);
}
