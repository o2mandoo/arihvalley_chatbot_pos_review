from typing import Literal

Domain = Literal["review", "sales"]


SALES_KEYWORDS = {
    "매출",
    "주문",
    "객단가",
    "매출액",
    "전환율",
    "매출분석",
    "판매량",
    "영업이익",
    "손익",
    "비용",
    "원가",
    "매출 데이터",
    "sales",
    "revenue",
    "order",
}

REVIEW_KEYWORDS = {
    "리뷰",
    "후기",
    "평점",
    "만족",
    "불만",
    "키워드",
    "웨이팅",
    "서비스",
    "맛",
    "시설",
    "review",
}


def classify_domain(message: str) -> Domain:
    lowered = message.lower()

    if any(token in lowered for token in REVIEW_KEYWORDS):
        return "review"
    if any(token in lowered for token in SALES_KEYWORDS):
        return "sales"
    # 현재 구현 우선순위: 기본은 리뷰 분석
    return "review"
