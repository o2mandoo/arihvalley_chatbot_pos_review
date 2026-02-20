/**
 * 2. 엑셀 → JSON 전처리 스크립트
 *
 * 복호화된 엑셀 파일을 읽어서 JSON으로 변환합니다.
 * 데이터 정제 및 검증 로직을 포함합니다.
 */

const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');

// 설정
const REVENUE_DATA_DIR = path.join(__dirname, '../../revenue-data');
const OUTPUT_DIR = path.join(__dirname, '../../revenue-data/processed');

// 컬럼 매핑 (엑셀 헤더 → 내부 필드명)
const COLUMN_MAP = {
  '주문기준일자': 'orderBaseDate',
  '주문번호': 'orderNumber',
  '주문시작시각': 'orderStartTime',
  '주문채널': 'orderChannel',
  '결제상태': 'paymentStatus',
  '카테고리': 'category',
  '상품명': 'productName',
  '수량': 'quantity',
  '상품가격': 'productPrice',
  '옵션': 'optionName',
  '옵션가격': 'optionPrice',
  '상품할인 금액': 'productDiscount',
  '주문할인 금액': 'orderDiscount',
  '실판매금액 \n (할인, 옵션 포함)': 'actualSalesAmount',
  '실판매금액': 'actualSalesAmount', // 대체 이름
  '과세여부': 'taxType',
  '부가세액': 'vatAmount',
};

function findDecryptedFiles() {
  if (!fs.existsSync(REVENUE_DATA_DIR)) {
    console.error('❌ revenue-data 폴더가 없습니다.');
    process.exit(1);
  }

  const files = fs.readdirSync(REVENUE_DATA_DIR);
  return files.filter(f =>
    f.endsWith('-decrypted.xlsx') &&
    !f.startsWith('~$')
  );
}

function parseExcelDate(value) {
  if (!value) return null;

  // 이미 Date 객체인 경우
  if (value instanceof Date) {
    return value;
  }

  // 문자열인 경우
  if (typeof value === 'string') {
    const date = new Date(value);
    if (!isNaN(date.getTime())) {
      return date;
    }
  }

  // Excel 시리얼 번호인 경우
  if (typeof value === 'number') {
    const excelEpoch = new Date(1899, 11, 30);
    const date = new Date(excelEpoch.getTime() + value * 86400000);
    return date;
  }

  return null;
}

function toNumber(val) {
  if (typeof val === 'number') return val;
  if (typeof val === 'string') {
    const num = parseFloat(val.replace(/,/g, ''));
    return isNaN(num) ? 0 : num;
  }
  return 0;
}

function cleanString(val) {
  if (!val) return '';
  return String(val).trim();
}

async function processExcelFile(filename) {
  const inputPath = path.join(REVENUE_DATA_DIR, filename);
  console.log(`\n📖 파일 처리 중: ${filename}`);

  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(inputPath);

  // "상품 주문 상세내역" 시트 찾기 (없으면 첫 번째 시트 사용)
  let worksheet = workbook.worksheets.find(ws => ws.name === '상품 주문 상세내역');
  if (!worksheet) {
    worksheet = workbook.worksheets[0];
  }

  if (!worksheet) {
    console.error(`❌ 워크시트를 찾을 수 없습니다: ${filename}`);
    return [];
  }

  console.log(`📋 시트: ${worksheet.name} (${worksheet.rowCount}행)`);

  // 헤더 행 찾기 (첫 번째 행)
  const headerRow = worksheet.getRow(1);
  const headers = [];
  headerRow.eachCell((cell, colNumber) => {
    headers[colNumber] = cleanString(cell.value);
  });

  console.log(`📋 컬럼 수: ${headers.filter(h => h).length}`);

  // 데이터 추출
  const records = [];
  let skippedRows = 0;

  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber <= 2) return; // 헤더(1행) + 설명행(2행) 스킵

    const record = {};
    let hasData = false;

    row.eachCell((cell, colNumber) => {
      const header = headers[colNumber];
      if (!header) return;

      const fieldName = COLUMN_MAP[header] || header;
      let value = cell.value;

      // ExcelJS의 rich text 처리
      if (value && typeof value === 'object' && value.richText) {
        value = value.richText.map(t => t.text).join('');
      }

      record[fieldName] = value;
      if (value !== null && value !== undefined && value !== '') {
        hasData = true;
      }
    });

    if (!hasData) {
      skippedRows++;
      return;
    }

    // 데이터 정제
    const cleanedRecord = {
      orderBaseDate: parseExcelDate(record.orderBaseDate),
      orderNumber: cleanString(record.orderNumber),
      orderStartTime: parseExcelDate(record.orderStartTime),
      orderChannel: cleanString(record.orderChannel) || '포스',
      paymentStatus: cleanString(record.paymentStatus) || '완료',
      category: cleanString(record.category) || '기타',
      productName: cleanString(record.productName),
      quantity: Math.max(1, Math.floor(toNumber(record.quantity))),
      productPrice: toNumber(record.productPrice),
      optionName: cleanString(record.optionName) || null,
      optionPrice: toNumber(record.optionPrice),
      productDiscount: toNumber(record.productDiscount),
      orderDiscount: toNumber(record.orderDiscount),
      actualSalesAmount: toNumber(record.actualSalesAmount),
      taxType: cleanString(record.taxType) || '과세',
      vatAmount: toNumber(record.vatAmount),
    };

    // 필수 필드 검증
    if (!cleanedRecord.orderNumber || !cleanedRecord.productName) {
      skippedRows++;
      return;
    }

    records.push(cleanedRecord);
  });

  console.log(`✅ 추출된 레코드: ${records.length}개`);
  if (skippedRows > 0) {
    console.log(`⏭️  스킵된 행: ${skippedRows}개`);
  }

  return records;
}

async function main() {
  console.log('📊 엑셀 → JSON 변환 시작\n');

  // 출력 폴더 생성
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const decryptedFiles = findDecryptedFiles();

  if (decryptedFiles.length === 0) {
    console.log('📭 처리할 파일이 없습니다.');
    console.log('   먼저 1-decrypt-excel.js를 실행하세요.');
    return { outputFiles: [] };
  }

  console.log(`📋 발견된 파일: ${decryptedFiles.length}개`);

  const outputFiles = [];
  let totalRecords = 0;

  for (const file of decryptedFiles) {
    const records = await processExcelFile(file);

    if (records.length === 0) continue;

    // JSON 파일로 저장
    const outputFilename = file.replace('-decrypted.xlsx', '.json');
    const outputPath = path.join(OUTPUT_DIR, outputFilename);

    fs.writeFileSync(outputPath, JSON.stringify(records, null, 2), 'utf-8');
    console.log(`💾 저장됨: ${outputFilename}`);

    outputFiles.push(outputFilename);
    totalRecords += records.length;
  }

  console.log(`\n✅ 변환 완료`);
  console.log(`   - 파일: ${outputFiles.length}개`);
  console.log(`   - 총 레코드: ${totalRecords.toLocaleString()}개`);

  return { outputFiles, totalRecords };
}

// 직접 실행 또는 모듈로 사용
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main, processExcelFile };
