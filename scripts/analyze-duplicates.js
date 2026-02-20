#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const PROCESSED_DIR = path.join(__dirname, "../revenue-data/processed");
const ARCHIVED_DIR = path.join(__dirname, "../revenue-data/archived");

function hasValue(value) {
  return value !== undefined && value !== null && value !== "";
}

function pick(record, keys) {
  for (const key of keys) {
    if (hasValue(record[key])) {
      return record[key];
    }
  }
  return null;
}

function findLatestJson() {
  const candidates = [PROCESSED_DIR, ARCHIVED_DIR]
    .filter((dir) => fs.existsSync(dir))
    .flatMap((dir) =>
      fs
        .readdirSync(dir)
        .filter((name) => name.endsWith(".json"))
        .map((name) => {
          const fullPath = path.join(dir, name);
          return {
            fullPath,
            mtimeMs: fs.statSync(fullPath).mtimeMs,
          };
        })
    )
    .sort((a, b) => b.mtimeMs - a.mtimeMs);

  return candidates[0]?.fullPath || null;
}

function loadData(inputPath) {
  const raw = fs.readFileSync(inputPath, "utf-8");
  const data = JSON.parse(raw);
  if (!Array.isArray(data)) {
    throw new Error("JSON root must be an array.");
  }
  return data;
}

function analyze(data) {
  const menus = new Map();
  const categories = new Set();
  const menuVariations = new Map();
  const orderGroups = new Map();

  for (const record of data) {
    const menuName = pick(record, ["productName", "상품명"]) || "(unknown-menu)";
    const category = pick(record, ["category", "카테고리"]) || "(unknown-category)";
    const unitPrice = Number(
      pick(record, ["productPrice", "상품가격", "unitPrice", "단가"]) || 0
    );
    const orderNumber = pick(record, ["orderNumber", "주문번호"]) || "(unknown-order)";

    if (!menus.has(menuName)) {
      menus.set(menuName, { category, unitPrice, count: 1 });
    } else {
      menus.get(menuName).count += 1;
    }

    categories.add(category);
    if (!menuVariations.has(menuName)) {
      menuVariations.set(menuName, new Set());
    }
    menuVariations.get(menuName).add(
      JSON.stringify({ category, unitPrice })
    );

    if (!orderGroups.has(orderNumber)) {
      orderGroups.set(orderNumber, 0);
    }
    orderGroups.set(orderNumber, orderGroups.get(orderNumber) + 1);
  }

  const inconsistent = [];
  for (const [menuName, variations] of menuVariations.entries()) {
    if (variations.size > 1) {
      inconsistent.push({
        menuName,
        variations: [...variations].map((value) => JSON.parse(value)),
      });
    }
  }

  return {
    totalRecords: data.length,
    uniqueMenus: menus.size,
    uniqueCategories: categories.size,
    categoryList: [...categories].sort(),
    inconsistentMenus: inconsistent.sort((a, b) =>
      a.menuName.localeCompare(b.menuName)
    ),
    uniqueOrders: orderGroups.size,
    averageItemsPerOrder:
      orderGroups.size > 0 ? data.length / orderGroups.size : 0,
  };
}

function printReport(filePath, result) {
  console.log("=== Duplicate Analysis ===");
  console.log(`Source file: ${filePath}`);
  console.log(`Total records: ${result.totalRecords.toLocaleString()}`);
  console.log(`Unique menus: ${result.uniqueMenus.toLocaleString()}`);
  console.log(`Unique categories: ${result.uniqueCategories.toLocaleString()}`);

  console.log("\n=== Menu inconsistency (same menu, different category/price) ===");
  console.log(`Total inconsistent menus: ${result.inconsistentMenus.length.toLocaleString()}`);
  result.inconsistentMenus.slice(0, 10).forEach((entry, index) => {
    console.log(`\n${index + 1}. ${entry.menuName}`);
    entry.variations.forEach((variation) => {
      console.log(
        `   - category=${variation.category}, unitPrice=${variation.unitPrice}`
      );
    });
  });

  console.log("\n=== Orders ===");
  console.log(`Unique orders: ${result.uniqueOrders.toLocaleString()}`);
  console.log(
    `Average items/order: ${result.averageItemsPerOrder.toFixed(1)}`
  );

  console.log("\n=== Categories ===");
  console.log(result.categoryList.join(", "));
}

function main() {
  const inputPathArg = process.argv[2];
  const inputPath = inputPathArg
    ? path.resolve(process.cwd(), inputPathArg)
    : findLatestJson();

  if (!inputPath || !fs.existsSync(inputPath)) {
    console.error(
      "No JSON file found. Pass a file path or ensure revenue-data/processed or revenue-data/archived contains JSON files."
    );
    process.exit(1);
  }

  const data = loadData(inputPath);
  const result = analyze(data);
  printReport(inputPath, result);
}

if (require.main === module) {
  main();
}
