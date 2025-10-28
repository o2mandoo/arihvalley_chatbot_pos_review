"""
리포트 생성 모듈 - 통계 분석 및 인사이트 추출
"""
import pandas as pd
import json
from collections import Counter
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class ReportGenerator:
    """리포트 생성 클래스"""

    def __init__(self, analyzed_df: pd.DataFrame):
        """
        Args:
            analyzed_df: LLM 분석이 완료된 데이터프레임
        """
        self.df = analyzed_df
        self.score_cols = ['시설_점수', '서비스_점수', '맛_점수']

        # 총점 계산
        if '총점' not in self.df.columns:
            self.df['총점'] = self.df[self.score_cols].sum(axis=1)

    def get_basic_stats(self) -> Dict:
        """기본 통계 추출"""
        stats = {
            'total_reviews': len(self.df),
            'avg_scores': {
                '시설': self.df['시설_점수'].mean(),
                '서비스': self.df['서비스_점수'].mean(),
                '맛': self.df['맛_점수'].mean(),
                '총점': self.df['총점'].mean()
            }
        }
        return stats

    def extract_keywords(self) -> Dict:
        """키워드 추출 (긍정/부정)"""
        all_positive = []
        all_negative = []

        for col in ['시설_긍정키워드', '서비스_긍정키워드', '맛_긍정키워드']:
            keywords = self.df[col].dropna().str.split(', ').sum()
            all_positive.extend([k.strip() for k in keywords if k.strip()])

        for col in ['시설_부정키워드', '서비스_부정키워드', '맛_부정키워드']:
            keywords = self.df[col].dropna().str.split(', ').sum()
            all_negative.extend([k.strip() for k in keywords if k.strip()])

        positive_counter = Counter(all_positive)
        negative_counter = Counter(all_negative)

        return {
            'positive': positive_counter.most_common(20),
            'negative': negative_counter.most_common(20)
        }

    def extract_customer_needs(self) -> List[tuple]:
        """고객 니즈 추출"""
        needs = self.df['고객니즈'].dropna().str.split(',').sum()
        needs_clean = [n.strip() for n in needs if n.strip()]
        needs_counter = Counter(needs_clean)
        return needs_counter.most_common(15)

    def analyze_menus(self) -> pd.DataFrame:
        """메뉴 분석"""
        menu_evaluations = []

        for idx, row in self.df.iterrows():
            menu_json = row['메뉴평가_JSON']
            if pd.notna(menu_json) and menu_json:
                try:
                    menus = json.loads(menu_json)
                    if isinstance(menus, list):
                        menu_evaluations.extend(menus)
                except:
                    pass

        # 메뉴별 집계
        menu_stats = {}
        for menu_eval in menu_evaluations:
            if isinstance(menu_eval, dict):
                menu_name = menu_eval.get('메뉴명', '')
                evaluation = menu_eval.get('평가', '')

                if menu_name and evaluation:
                    if menu_name not in menu_stats:
                        menu_stats[menu_name] = {'긍정': 0, '부정': 0, '중립': 0}

                    if evaluation in menu_stats[menu_name]:
                        menu_stats[menu_name][evaluation] += 1

        if not menu_stats:
            return pd.DataFrame()

        menu_df = pd.DataFrame(menu_stats).T
        menu_df['총_언급'] = menu_df.sum(axis=1)
        menu_df['긍정_비율'] = (menu_df['긍정'] / menu_df['총_언급'] * 100).round(1)
        menu_df = menu_df.sort_values('총_언급', ascending=False)

        return menu_df

    def get_hidden_complaints(self) -> List[str]:
        """숨은 불만 추출"""
        complaints = self.df['숨은불만'].dropna()
        complaints = complaints[complaints != '']
        return complaints.tolist()

    def get_revisit_intent_distribution(self) -> Dict:
        """재방문 의도 분포"""
        revisit_dist = self.df['재방문의도'].value_counts()
        return revisit_dist.to_dict()

    def generate_full_report(self) -> str:
        """전체 리포트 생성"""
        report_lines = []
        report_lines.append("=" * 100)
        report_lines.append("🎯 리뷰 분석 종합 리포트")
        report_lines.append("=" * 100)

        # 기본 통계
        stats = self.get_basic_stats()
        report_lines.append(f"\n📊 분석 개요")
        report_lines.append(f"  - 총 리뷰 수: {stats['total_reviews']:,}건")

        report_lines.append(f"\n🏆 전체 평균 점수")
        for category, score in stats['avg_scores'].items():
            report_lines.append(f"  - {category}: {score:.2f}/5")

        # 키워드
        keywords = self.extract_keywords()
        report_lines.append(f"\n🟢 주요 강점 (긍정 키워드 Top 15):")
        for keyword, count in keywords['positive'][:15]:
            report_lines.append(f"  {count}회 - {keyword}")

        report_lines.append(f"\n🔴 주요 약점 (부정 키워드 Top 15):")
        for keyword, count in keywords['negative'][:15]:
            report_lines.append(f"  {count}회 - {keyword}")

        # 고객 니즈
        needs = self.extract_customer_needs()
        report_lines.append(f"\n💡 핵심 고객 니즈 Top 10:")
        for need, count in needs[:10]:
            report_lines.append(f"  {count}회 - {need}")

        # 메뉴 분석
        menu_df = self.analyze_menus()
        if not menu_df.empty:
            report_lines.append(f"\n🍽️  인기 메뉴 Top 15:")
            for idx, (menu, row) in enumerate(menu_df.head(15).iterrows(), 1):
                report_lines.append(
                    f"  {idx}. {menu}: {row['총_언급']:.0f}회 (긍정 {row['긍정_비율']:.1f}%)"
                )

        # 숨은 불만
        complaints = self.get_hidden_complaints()
        report_lines.append(f"\n🔍 숨은 불만")
        report_lines.append(f"  - 발견: {len(complaints)}건 ({len(complaints)/len(self.df)*100:.1f}%)")
        if len(complaints) > 0:
            report_lines.append(f"\n  대표 사례 5개:")
            for idx, complaint in enumerate(complaints[:5], 1):
                report_lines.append(f"  {idx}. {complaint}")

        # 재방문 의도
        revisit_dist = self.get_revisit_intent_distribution()
        report_lines.append(f"\n🔄 재방문 의도")
        for intent, count in revisit_dist.items():
            report_lines.append(f"  - {intent}: {count}건 ({count/len(self.df)*100:.1f}%)")

        # 액션 플랜
        report_lines.append("\n" + "=" * 100)
        report_lines.append("🎯 실행 가능한 액션 플랜")
        report_lines.append("=" * 100)

        report_lines.append("\n1️⃣ 최우선 개선 과제 (부정 언급 빈도 기준)")
        for idx, (issue, count) in enumerate(keywords['negative'][:5], 1):
            impact = "높음" if count > 20 else "중간" if count > 10 else "낮음"
            report_lines.append(f"  [{idx}] {issue} (언급 {count}회, 영향도: {impact})")

        report_lines.append("\n2️⃣ 강점 메뉴 (마케팅 강화 대상)")
        if not menu_df.empty:
            top_rated = menu_df[menu_df['긍정_비율'] >= 85].sort_values('총_언급', ascending=False)
            for menu, row in top_rated.head(5).iterrows():
                report_lines.append(
                    f"  ⭐ {menu}: 긍정 {row['긍정_비율']:.1f}% (언급 {row['총_언급']:.0f}회)"
                )

        report_lines.append("\n" + "=" * 100)
        report_lines.append("✅ 리포트 생성 완료")
        report_lines.append("=" * 100)

        return "\n".join(report_lines)

    def save_report(self, filepath: str) -> bool:
        """리포트를 파일로 저장"""
        try:
            report = self.generate_full_report()
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"리포트 저장 완료: {filepath}")
            return True
        except Exception as e:
            logger.error(f"리포트 저장 실패: {e}")
            return False
