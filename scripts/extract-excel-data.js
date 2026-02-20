#!/usr/bin/env node

console.warn(
  "[DEPRECATED] scripts/extract-excel-data.js -> scripts/pipeline/2-excel-to-json.js"
);

const { main } = require("./pipeline/2-excel-to-json");

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
