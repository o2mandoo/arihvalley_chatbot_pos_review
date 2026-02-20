const { PrismaClient } = require('@prisma/client');
const { PrismaPg } = require('@prisma/adapter-pg');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// Create PostgreSQL pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Create adapter
const adapter = new PrismaPg(pool);

const prisma = new PrismaClient({
  adapter,
});

async function importData() {
  console.log('📖 Loading sales data...');
  const rawData = JSON.parse(
    fs.readFileSync(path.join(__dirname, '../public/sales-data.json'), 'utf-8')
  );
  console.log(`✓ Loaded ${rawData.length} records`);

  // 1. 지점 생성 (현재는 단일 지점)
  console.log('\n📍 Creating branch...');
  const branch = await prisma.branch.upsert({
    where: { name: '왕십리한양대점' },
    update: {},
    create: {
      name: '왕십리한양대점',
      areaType: '대학가',
    },
  });
  console.log(`✓ Branch: ${branch.name} (ID: ${branch.id})`);

  // 2. 카테고리 추출 및 생성
  console.log('\n📂 Creating categories...');
  const categoryNames = [...new Set(rawData.map(r => r['카테고리']))];
  const categoryMap = new Map();

  for (const name of categoryNames) {
    const category = await prisma.category.upsert({
      where: { name },
      update: {},
      create: { name },
    });
    categoryMap.set(name, category.id);
  }
  console.log(`✓ Created ${categoryMap.size} categories`);

  // 3. 메뉴 추출 및 생성
  console.log('\n🍽️  Creating menus...');
  const menuData = new Map();
  rawData.forEach(r => {
    const menuName = r['상품명'];
    if (!menuData.has(menuName)) {
      menuData.set(menuName, {
        name: menuName,
        category: r['카테고리'],
        basePrice: r['상품가격'],
      });
    }
  });

  const menuMap = new Map();
  for (const [name, data] of menuData) {
    const menu = await prisma.menu.upsert({
      where: { name },
      update: {},
      create: {
        name,
        categoryId: categoryMap.get(data.category),
        basePrice: Math.max(0, data.basePrice), // 음수 가격 방지
      },
    });
    menuMap.set(name, menu.id);
  }
  console.log(`✓ Created ${menuMap.size} menus`);

  // 4. 주문 및 주문 상세 생성
  console.log('\n📝 Creating orders and items...');

  // 주문번호 + 날짜로 그룹핑
  const orderGroups = new Map();
  rawData.forEach(r => {
    const orderDate = new Date(r['주문기준일자']);
    const dateStr = orderDate.toISOString().split('T')[0];
    const key = `${r['주문번호']}_${dateStr}`;

    if (!orderGroups.has(key)) {
      orderGroups.set(key, {
        orderNumber: r['주문번호'],
        orderDate: new Date(r['주문시작시각']),
        orderChannel: r['주문채널'],
        orderStatus: r['결제상태'],
        items: [],
      });
    }

    orderGroups.get(key).items.push({
      menuName: r['상품명'],
      quantity: r['수량'],
      unitPrice: r['상품가격'],
      optionName: r['옵션'] || null,
      optionPrice: r['옵션가격'] || 0,
      productDiscount: r['상품할인 금액'] || 0,
      orderDiscount: r['주문할인 금액'] || 0,
      totalAmount: r['실판매금액 \n (할인, 옵션 포함)'] || 0,
      taxType: r['과세여부'] || '과세',
      vatAmount: r['부가세액'] || 0,
    });
  });

  console.log(`✓ Found ${orderGroups.size} unique orders`);

  let orderCount = 0;
  let itemCount = 0;

  for (const [key, orderData] of orderGroups) {
    try {
      // 주문 생성
      const order = await prisma.order.upsert({
        where: {
          branchId_orderNumber_orderDate: {
            branchId: branch.id,
            orderNumber: orderData.orderNumber,
            orderDate: orderData.orderDate,
          },
        },
        update: {},
        create: {
          orderNumber: orderData.orderNumber,
          branchId: branch.id,
          orderDate: orderData.orderDate,
          orderChannel: orderData.orderChannel,
          orderStatus: orderData.orderStatus,
        },
      });

      // 주문 상세 생성
      for (const item of orderData.items) {
        const menuId = menuMap.get(item.menuName);
        if (!menuId) {
          console.warn(`⚠️  Menu not found: ${item.menuName}`);
          continue;
        }

        await prisma.orderItem.create({
          data: {
            orderId: order.id,
            menuId: menuId,
            quantity: item.quantity,
            unitPrice: item.unitPrice,
            optionName: item.optionName,
            optionPrice: item.optionPrice,
            productDiscount: item.productDiscount,
            orderDiscount: item.orderDiscount,
            totalAmount: item.totalAmount,
            taxType: item.taxType,
            vatAmount: item.vatAmount,
          },
        });
        itemCount++;
      }

      orderCount++;
      if (orderCount % 100 === 0) {
        console.log(`  Processed ${orderCount} orders...`);
      }
    } catch (error) {
      console.error(`❌ Error processing order ${key}:`, error.message);
    }
  }

  console.log(`\n✅ Import completed!`);
  console.log(`   - Branches: 1`);
  console.log(`   - Categories: ${categoryMap.size}`);
  console.log(`   - Menus: ${menuMap.size}`);
  console.log(`   - Orders: ${orderCount}`);
  console.log(`   - Order Items: ${itemCount}`);
}

importData()
  .catch(console.error)
  .finally(() => prisma.$disconnect());
