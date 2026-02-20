#!/usr/bin/env node
/**
 * 매출 데이터 파이프라인 실행 스크립트
 *
 * revenue-data/ 폴더의 엑셀 파일을 자동으로:
 * 1. 비밀번호 해제
 * 2. JSON으로 변환
 * 3. PostgreSQL에 적재 (중복 제외)
 */

require('dotenv/config');
const { main: decryptExcel } = require('./1-decrypt-excel');
const { main: excelToJson } = require('./2-excel-to-json');
const { main: importToDb } = require('./3-import-to-db');

async function runPipeline() {
  const startTime = Date.now();

  console.log('═'.repeat(60));
  console.log('🚀 매출 데이터 파이프라인 시작');
  console.log('═'.repeat(60));
  console.log(`📅 시작 시간: ${new Date().toLocaleString('ko-KR')}`);
  console.log('');

  try {
    // Step 1: 비밀번호 해제
    console.log('─'.repeat(60));
    console.log('📌 Step 1: 엑셀 비밀번호 해제');
    console.log('─'.repeat(60));
    await decryptExcel();

    // Step 2: JSON 변환
    console.log('\n' + '─'.repeat(60));
    console.log('📌 Step 2: 엑셀 → JSON 변환');
    console.log('─'.repeat(60));
    await excelToJson();

    // Step 3: DB 적재
    console.log('\n' + '─'.repeat(60));
    console.log('📌 Step 3: PostgreSQL 적재');
    console.log('─'.repeat(60));
    await importToDb();

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log('\n' + '═'.repeat(60));
    console.log('✅ 파이프라인 완료!');
    console.log('═'.repeat(60));
    console.log(`⏱️  소요 시간: ${duration}초`);
    console.log(`📅 완료 시간: ${new Date().toLocaleString('ko-KR')}`);

  } catch (error) {
    console.error('\n❌ 파이프라인 실패:', error.message);
    process.exit(1);
  }
}

// 실행
runPipeline();
