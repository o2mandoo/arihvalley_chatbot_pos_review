-- sales_records VIEW 생성
-- 정규화된 테이블(orders, order_items, branches, menus, categories)을 조인하여
-- Analytics API가 기대하는 비정규화된 형태로 제공

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
    oi.total_amount::float as amount,  -- 실판매금액 직접 사용 (unit_price가 총액인 경우 중복계산 방지)
    (oi.product_discount + oi.order_discount)::float as discount_amount,
    '카드' as payment_method,  -- 기본값 (원본 데이터에 없는 경우)
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

-- VIEW에 인덱스는 직접 생성 불가능하지만, 기반 테이블의 인덱스가 활용됨
-- orders(order_date), branches(name), categories(name), orders(order_status) 인덱스 활용
