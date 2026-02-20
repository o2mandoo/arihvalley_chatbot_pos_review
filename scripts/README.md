# Scripts layout

## Active scripts

- `pipeline/1-decrypt-excel.js`: decrypt encrypted Excel files in `revenue-data/`
- `pipeline/2-excel-to-json.js`: transform decrypted Excel into JSON in `revenue-data/processed/`
- `pipeline/3-import-to-db.js`: import processed JSON into normalized PostgreSQL tables
- `pipeline/4-seed-staff-data.js`: optional seed for staff/schedule demo data
- `pipeline/run-pipeline.js`: run step 1 -> 2 -> 3 in one command

## Wrappers for backward compatibility

Top-level script files are wrappers that redirect to the active pipeline scripts:

- `extract-excel-data.js` -> `pipeline/2-excel-to-json.js`
- `import-to-db.js` -> `pipeline/3-import-to-db.js`
- `import-normalized-data.js` -> `pipeline/3-import-to-db.js`
- `seed-staff-data.js` -> `pipeline/4-seed-staff-data.js`

`analyze-duplicates.js` is kept as a utility and now reads from `revenue-data/processed` (or a file path argument).

## Legacy code

Original legacy implementations are preserved in `scripts/legacy/`.
