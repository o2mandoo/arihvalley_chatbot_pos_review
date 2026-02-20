const data = require('../public/sales-data.json');

// 중복 확인
const menus = new Map();
const categories = new Set();

data.forEach(r => {
  const menuName = r['상품명'];
  const category = r['카테고리'];
  const price = r['상품가격'];

  if (!menus.has(menuName)) {
    menus.set(menuName, { category, price, count: 1 });
  } else {
    menus.get(menuName).count++;
  }
  categories.add(category);
});

console.log('=== 중복 분석 ===');
console.log('총 레코드:', data.length);
console.log('고유 메뉴 수:', menus.size);
console.log('고유 카테고리 수:', categories.size);

// 같은 메뉴가 다른 카테고리/가격으로 등록된 경우 확인
const menuPriceMap = new Map();
data.forEach(r => {
  const key = r['상품명'];
  if (!menuPriceMap.has(key)) {
    menuPriceMap.set(key, new Set());
  }
  menuPriceMap.get(key).add(JSON.stringify({ cat: r['카테고리'], price: r['상품가격'] }));
});

console.log('\n=== 같은 메뉴, 다른 가격/카테고리 ===');
let inconsistent = 0;
menuPriceMap.forEach((variations, menuName) => {
  if (variations.size > 1) {
    inconsistent++;
    if (inconsistent <= 5) {
      console.log(`\n"${menuName}":`);
      variations.forEach(v => console.log('  -', JSON.parse(v)));
    }
  }
});
console.log(`\n불일치 메뉴 총 ${inconsistent}개`);

// 카테고리 목록
console.log('\n=== 카테고리 목록 ===');
console.log([...categories].join(', '));

// 주문번호별 분석
const orders = new Map();
data.forEach(r => {
  const orderId = r['주문번호'];
  if (!orders.has(orderId)) {
    orders.set(orderId, []);
  }
  orders.get(orderId).push(r);
});

console.log('\n=== 주문 분석 ===');
console.log('고유 주문번호 수:', orders.size);
console.log('평균 주문당 상품 수:', (data.length / orders.size).toFixed(1));
