/**
 * 1. 엑셀 파일 비밀번호 해제 스크립트
 *
 * revenue-data/ 폴더의 암호화된 엑셀 파일을 해제하여
 * 같은 폴더에 -decrypted.xlsx 파일로 저장합니다.
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// 설정
const REVENUE_DATA_DIR = path.join(__dirname, '../../revenue-data');
const EXCEL_PASSWORD = process.env.EXCEL_PASSWORD || '1234'; // 기본 비밀번호

function findEncryptedFiles() {
  if (!fs.existsSync(REVENUE_DATA_DIR)) {
    console.error('❌ revenue-data 폴더가 없습니다.');
    process.exit(1);
  }

  const files = fs.readdirSync(REVENUE_DATA_DIR);

  // 암호화된 파일 찾기 (decrypted가 아닌 xlsx 파일)
  const encryptedFiles = files.filter(f =>
    f.endsWith('.xlsx') &&
    !f.includes('-decrypted') &&
    !f.startsWith('~$') // 임시 파일 제외
  );

  return encryptedFiles;
}

function decryptFile(filename) {
  const inputPath = path.join(REVENUE_DATA_DIR, filename);
  const outputFilename = filename.replace('.xlsx', '-decrypted.xlsx');
  const outputPath = path.join(REVENUE_DATA_DIR, outputFilename);

  // 이미 해제된 파일이 있으면 스킵
  if (fs.existsSync(outputPath)) {
    console.log(`⏭️  이미 해제됨: ${outputFilename}`);
    return outputFilename;
  }

  console.log(`🔓 비밀번호 해제 중: ${filename}`);

  try {
    // msoffice-crypto 사용 (npx로 실행)
    execSync(
      `npx -y msoffice-crypto -d -p "${EXCEL_PASSWORD}" "${inputPath}" "${outputPath}"`,
      { stdio: 'pipe' }
    );

    console.log(`✅ 해제 완료: ${outputFilename}`);
    return outputFilename;
  } catch (error) {
    // 이미 암호화가 안된 파일일 수 있음
    console.log(`⚠️  비밀번호 없는 파일일 수 있음, 복사합니다: ${filename}`);
    fs.copyFileSync(inputPath, outputPath);
    return outputFilename;
  }
}

async function main() {
  console.log('🔐 엑셀 비밀번호 해제 시작\n');
  console.log(`📁 대상 폴더: ${REVENUE_DATA_DIR}`);
  console.log(`🔑 비밀번호: ${EXCEL_PASSWORD}\n`);

  const encryptedFiles = findEncryptedFiles();

  if (encryptedFiles.length === 0) {
    console.log('📭 처리할 파일이 없습니다.');
    return { decryptedFiles: [] };
  }

  console.log(`📋 발견된 파일: ${encryptedFiles.length}개\n`);

  const decryptedFiles = [];
  for (const file of encryptedFiles) {
    const decryptedFile = decryptFile(file);
    if (decryptedFile) {
      decryptedFiles.push(decryptedFile);
    }
  }

  console.log(`\n✅ 총 ${decryptedFiles.length}개 파일 처리 완료`);

  return { decryptedFiles };
}

// 직접 실행 또는 모듈로 사용
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main, findEncryptedFiles, decryptFile };
