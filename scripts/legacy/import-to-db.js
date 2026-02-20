require('dotenv/config');
const { PrismaClient } = require('@prisma/client');
const { PrismaPg } = require('@prisma/adapter-pg');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// Connect to PostgreSQL database using adapter
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });

// Transform raw data to match our schema
function transformRawData(raw) {
  if (!raw.주문기준일자 || !raw.주문번호 || !raw.상품명) {
    return null;
  }

  const toNumber = (val) => {
    if (typeof val === 'number') return val;
    const num = parseFloat(val || '0');
    return isNaN(num) ? 0 : num;
  };

  // Parse date and time
  const orderDate = raw.주문시작시각
    ? new Date(raw.주문시작시각).toISOString().split('T')[0]
    : new Date(raw.주문기준일자).toISOString().split('T')[0];

  const orderTime = raw.주문시작시각
    ? new Date(raw.주문시작시각).toISOString().split('T')[1].split('.')[0]
    : '00:00:00';

  // Category mapping
  const categoryMap = {
    '주류': '주류',
    '음료': '음료',
    '전,튀김안주': '전',
    '대표안주': '대표안주',
    '세트': '세트',
    '하이볼': '하이볼',
    '옵션': '옵션',
    '튀김안주': '튀김안주',
  };

  const category = categoryMap[raw.카테고리] || raw.카테고리 || '기타';

  return {
    orderId: `ORD_${raw.주문번호}`,
    orderDate,
    orderTime,
    branchName: '왕십리한양대점',
    areaType: '대학가',
    menuName: raw.상품명,
    category,
    quantity: Math.max(1, Math.floor(toNumber(raw.수량))),
    price: toNumber(raw.상품가격),
    amount: toNumber(raw['실판매금액 \n (할인, 옵션 포함)']),
    discountAmount: toNumber(raw['상품할인 금액']) + toNumber(raw['주문할인 금액']),
    paymentMethod: '카드',
    orderChannel: raw.주문채널 === '포스' ? '포스' : '테이블오더',
    orderStatus: raw.결제상태 === '완료' ? '완료' : '취소',
  };
}

async function importData() {
  try {
    console.log('📖 Loading sales data...');
    const dataPath = path.join(__dirname, '../public/sales-data.json');
    const rawData = JSON.parse(fs.readFileSync(dataPath, 'utf-8'));

    console.log(`✓ Loaded ${rawData.length.toLocaleString()} raw records`);

    // Transform data
    console.log('🔄 Transforming data...');
    const transformedData = rawData
      .map(transformRawData)
      .filter(record => record !== null);

    console.log(`✓ Transformed ${transformedData.length.toLocaleString()} valid records`);

    // Clear existing data
    console.log('🗑️  Clearing existing data...');
    await prisma.salesRecord.deleteMany({});
    console.log('✓ Database cleared');

    // Import in batches
    const BATCH_SIZE = 1000;
    const totalBatches = Math.ceil(transformedData.length / BATCH_SIZE);

    console.log(`📥 Importing data in ${totalBatches} batches...`);

    for (let i = 0; i < transformedData.length; i += BATCH_SIZE) {
      const batch = transformedData.slice(i, i + BATCH_SIZE);
      const batchNum = Math.floor(i / BATCH_SIZE) + 1;

      await prisma.salesRecord.createMany({
        data: batch,
        skipDuplicates: true,
      });

      process.stdout.write(`\r  Batch ${batchNum}/${totalBatches} (${((batchNum / totalBatches) * 100).toFixed(1)}%)`);
    }

    console.log('\n✅ Data import complete!');

    // Show stats
    const count = await prisma.salesRecord.count();
    console.log(`\n📊 Database statistics:`);
    console.log(`   Total records: ${count.toLocaleString()}`);

    const dateRange = await prisma.salesRecord.aggregate({
      _min: { orderDate: true },
      _max: { orderDate: true },
    });
    console.log(`   Date range: ${dateRange._min.orderDate} to ${dateRange._max.orderDate}`);

  } catch (error) {
    console.error('❌ Import failed:', error);
    throw error;
  } finally {
    await prisma.$disconnect();
  }
}

importData();
