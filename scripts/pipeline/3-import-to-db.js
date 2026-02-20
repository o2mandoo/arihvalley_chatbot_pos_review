/**
 * 3. JSON → PostgreSQL 적재 스크립트
 *
 * 전처리된 JSON 파일을 PostgreSQL에 적재합니다.
 * 중복 데이터는 자동으로 제외됩니다.
 */

require('dotenv/config');
const { PrismaClient } = require('@prisma/client');
const { PrismaPg } = require('@prisma/adapter-pg');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// 설정
const PROCESSED_DIR = path.join(__dirname, '../../revenue-data/processed');
const ARCHIVE_DIR = path.join(__dirname, '../../revenue-data/archived');

// PostgreSQL 연결
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

const SALES_RECORDS_VIEW_SQL = `
CREATE OR REPLACE VIEW sales_records AS
SELECT
    oi.id::text as id,
    o.order_number as order_id,
    to_char(o.order_date, 'YYYY-MM-DD') as order_date,
    to_char(o.order_date, 'HH24:MI:SS') as order_time,
    b.name as branch_name,
    b.area_type as area_type,
    m.name as menu_name,
    c.name as category,
    oi.quantity as quantity,
    oi.unit_price::float as price,
    oi.total_amount::float as amount,
    (oi.product_discount + oi.order_discount)::float as discount_amount,
    '카드' as payment_method,
    o.order_channel as order_channel,
    o.order_status as order_status,
    oi.created_at as created_at,
    o.updated_at as updated_at,
    -- LLM 친화 한글 컬럼 별칭
    oi.id::text as "레코드ID",
    o.order_number as "주문번호",
    to_char(o.order_date, 'YYYY-MM-DD') as "주문일자",
    to_char(o.order_date, 'HH24:MI:SS') as "주문시간",
    b.name as "지점명",
    b.area_type as "지역유형",
    m.name as "메뉴명",
    c.name as "카테고리",
    oi.quantity as "수량",
    oi.unit_price::float as "단가",
    oi.total_amount::float as "실판매금액",
    (oi.product_discount + oi.order_discount)::float as "할인금액",
    '카드' as "결제수단",
    o.order_channel as "주문채널",
    o.order_status as "주문상태",
    oi.created_at as "생성일시",
    o.updated_at as "수정일시"
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN branches b ON o.branch_id = b.id
JOIN menus m ON oi.menu_id = m.id
JOIN categories c ON m.category_id = c.id;
`;

function findProcessedFiles() {
  if (!fs.existsSync(PROCESSED_DIR)) {
    console.error('❌ processed 폴더가 없습니다.');
    console.log('   먼저 2-excel-to-json.js를 실행하세요.');
    return [];
  }

  return fs.readdirSync(PROCESSED_DIR).filter(f => f.endsWith('.json'));
}

async function ensureBranch(branchName = '왕십리한양대점', areaType = '대학가') {
  return prisma.branch.upsert({
    where: { name: branchName },
    update: {},
    create: { name: branchName, areaType },
  });
}

async function ensureCategory(categoryName) {
  return prisma.category.upsert({
    where: { name: categoryName },
    update: {},
    create: { name: categoryName },
  });
}

async function ensureMenu(menuName, categoryId, basePrice) {
  return prisma.menu.upsert({
    where: { name: menuName },
    update: {},
    create: {
      name: menuName,
      categoryId,
      basePrice: Math.max(0, basePrice),
    },
  });
}

async function importFile(filename) {
  const filePath = path.join(PROCESSED_DIR, filename);
  console.log(`\n📥 파일 적재 중: ${filename}`);

  const records = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  console.log(`   레코드 수: ${records.length.toLocaleString()}개`);

  // 지점 생성
  const branch = await ensureBranch();

  // 카테고리 캐시
  const categoryCache = new Map();
  const menuCache = new Map();

  // 주문별 그룹핑
  const orderGroups = new Map();

  for (const record of records) {
    const orderDate = new Date(record.orderStartTime || record.orderBaseDate);
    const dateStr = orderDate.toISOString().split('T')[0];
    const key = `${record.orderNumber}_${dateStr}`;

    if (!orderGroups.has(key)) {
      orderGroups.set(key, {
        orderNumber: record.orderNumber,
        orderDate: orderDate,
        orderChannel: record.orderChannel,
        orderStatus: record.paymentStatus,
        items: [],
      });
    }

    orderGroups.get(key).items.push(record);
  }

  console.log(`   고유 주문 수: ${orderGroups.size.toLocaleString()}개`);

  // 통계
  let newOrders = 0;
  let skippedOrders = 0;
  let newItems = 0;
  let errors = 0;

  for (const [key, orderData] of orderGroups) {
    try {
      // 중복 체크: 이미 존재하는 주문인지 확인
      const existingOrder = await prisma.order.findUnique({
        where: {
          branchId_orderNumber_orderDate: {
            branchId: branch.id,
            orderNumber: orderData.orderNumber,
            orderDate: orderData.orderDate,
          },
        },
      });

      if (existingOrder) {
        skippedOrders++;
        continue; // 중복 주문 스킵
      }

      // 주문 생성
      const order = await prisma.order.create({
        data: {
          orderNumber: orderData.orderNumber,
          branchId: branch.id,
          orderDate: orderData.orderDate,
          orderChannel: orderData.orderChannel,
          orderStatus: orderData.orderStatus,
        },
      });

      newOrders++;

      // 주문 상세 생성
      for (const item of orderData.items) {
        // 카테고리 캐시 또는 생성
        let categoryId = categoryCache.get(item.category);
        if (!categoryId) {
          const category = await ensureCategory(item.category);
          categoryId = category.id;
          categoryCache.set(item.category, categoryId);
        }

        // 메뉴 캐시 또는 생성
        let menuId = menuCache.get(item.productName);
        if (!menuId) {
          const menu = await ensureMenu(item.productName, categoryId, item.productPrice);
          menuId = menu.id;
          menuCache.set(item.productName, menuId);
        }

        await prisma.orderItem.create({
          data: {
            orderId: order.id,
            menuId: menuId,
            quantity: item.quantity,
            unitPrice: item.productPrice,
            optionName: item.optionName,
            optionPrice: item.optionPrice,
            productDiscount: item.productDiscount,
            orderDiscount: item.orderDiscount,
            totalAmount: item.actualSalesAmount,
            taxType: item.taxType,
            vatAmount: item.vatAmount,
          },
        });

        newItems++;
      }

      // 진행 상황 표시
      if ((newOrders + skippedOrders) % 500 === 0) {
        process.stdout.write(`\r   처리 중: ${newOrders + skippedOrders}/${orderGroups.size} 주문...`);
      }
    } catch (error) {
      errors++;
      if (errors <= 5) {
        console.error(`\n   ⚠️ 오류 (${key}): ${error.message}`);
      }
    }
  }

  console.log(`\n   ✅ 완료:`);
  console.log(`      - 새 주문: ${newOrders.toLocaleString()}개`);
  console.log(`      - 중복 스킵: ${skippedOrders.toLocaleString()}개`);
  console.log(`      - 새 항목: ${newItems.toLocaleString()}개`);
  if (errors > 0) {
    console.log(`      - 오류: ${errors}개`);
  }

  return { newOrders, skippedOrders, newItems, errors };
}

async function archiveProcessedFile(filename) {
  if (!fs.existsSync(ARCHIVE_DIR)) {
    fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
  }

  const sourcePath = path.join(PROCESSED_DIR, filename);
  const destPath = path.join(ARCHIVE_DIR, filename);

  fs.renameSync(sourcePath, destPath);
  console.log(`   📦 아카이브됨: ${filename}`);
}

async function main() {
  console.log('🗄️  PostgreSQL 적재 시작');
  console.log(`📁 DB: ${process.env.DATABASE_URL?.split('@')[1] || 'configured'}\n`);

  const processedFiles = findProcessedFiles();

  if (processedFiles.length === 0) {
    console.log('📭 처리할 파일이 없습니다.');
    return;
  }

  console.log(`📋 처리할 파일: ${processedFiles.length}개`);

  // 총 통계
  let totalNewOrders = 0;
  let totalSkipped = 0;
  let totalNewItems = 0;

  for (const file of processedFiles) {
    try {
      const result = await importFile(file);
      totalNewOrders += result.newOrders;
      totalSkipped += result.skippedOrders;
      totalNewItems += result.newItems;

      // 처리 완료된 파일 아카이브
      await archiveProcessedFile(file);
    } catch (error) {
      console.error(`❌ 파일 처리 실패 (${file}): ${error.message}`);
    }
  }

  await ensureSalesRecordsView();

  console.log('\n' + '='.repeat(50));
  console.log('📊 전체 적재 결과');
  console.log('='.repeat(50));
  console.log(`   ✅ 새 주문: ${totalNewOrders.toLocaleString()}개`);
  console.log(`   ⏭️  중복 스킵: ${totalSkipped.toLocaleString()}개`);
  console.log(`   📦 새 항목: ${totalNewItems.toLocaleString()}개`);

  // DB 통계 출력
  const orderCount = await prisma.order.count();
  const itemCount = await prisma.orderItem.count();
  console.log('\n📈 현재 DB 상태');
  console.log(`   - 총 주문: ${orderCount.toLocaleString()}개`);
  console.log(`   - 총 항목: ${itemCount.toLocaleString()}개`);
}

async function ensureSalesRecordsView() {
  try {
    await prisma.$executeRawUnsafe(SALES_RECORDS_VIEW_SQL);
    console.log('\n🧩 sales_records 뷰 갱신 완료 (한글 컬럼 포함)');
  } catch (error) {
    console.error(`\n❌ sales_records 뷰 갱신 실패: ${error.message}`);
  }
}

// 직접 실행 또는 모듈로 사용
if (require.main === module) {
  main()
    .catch(console.error)
    .finally(() => prisma.$disconnect());
}

module.exports = { main, importFile };
