const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');

async function extractExcelData() {
  const workbook = new ExcelJS.Workbook();
  const filePath = path.join(__dirname, '../매출리포트-251209153904-decrypted.xlsx');

  console.log('📖 Reading Excel file...');
  await workbook.xlsx.readFile(filePath);

  // "상품 주문 상세내역" 워크시트 가져오기
  const worksheet = workbook.worksheets[4]; // Index 4
  console.log(`📄 Worksheet: ${worksheet.name}`);

  // 헤더 추출
  const headerRow = worksheet.getRow(1);
  const headers = [];
  headerRow.eachCell((cell, colNumber) => {
    headers[colNumber] = cell.value;
  });

  console.log('📋 Headers:', headers.filter(Boolean));

  // 데이터 추출
  const data = [];
  let rowCount = 0;

  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return; // Skip header row

    const rowData = {};
    row.eachCell((cell, colNumber) => {
      const header = headers[colNumber];
      if (header) {
        rowData[header] = cell.value;
      }
    });

    // 빈 행 스킵
    if (Object.keys(rowData).length > 0 && rowData['주문기준일자']) {
      data.push(rowData);
      rowCount++;
    }
  });

  console.log(`✓ Extracted ${rowCount.toLocaleString()} records`);

  // JSON 파일로 저장
  const outputPath = path.join(__dirname, '../public/sales-data.json');
  fs.writeFileSync(outputPath, JSON.stringify(data, null, 2));

  console.log(`✓ Saved to ${outputPath}`);
  console.log('\nSample record:');
  console.log(JSON.stringify(data[0], null, 2));
}

extractExcelData().catch(console.error);
