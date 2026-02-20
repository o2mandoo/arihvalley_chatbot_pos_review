"""
LLM 기반 리뷰 분석 모듈
"""
import os
import json
import time
import re
import pandas as pd
from typing import Dict, Optional, List
from openai import OpenAI
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from tqdm.auto import tqdm
import logging

logger = logging.getLogger(__name__)


def load_env_from_shell_rc(var_name: str) -> Optional[str]:
    """zsh 설정 파일에서 환경변수를 찾아 현재 프로세스 환경변수로 로드"""
    candidates = [
        os.path.expanduser("~/.zshrc"),
        os.path.expanduser("~/.zshenv"),
        os.path.expanduser("~/.zprofile"),
    ]

    pattern = re.compile(rf"^(export\s+)?{re.escape(var_name)}\s*=\s*(.+)$")

    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    match = pattern.match(stripped)
                    if not match:
                        continue
                    value = match.group(2).strip()
                    if (value.startswith("'") and value.endswith("'")) or (
                        value.startswith('"') and value.endswith('"')
                    ):
                        value = value[1:-1]
                    if value:
                        os.environ[var_name] = value
                        return value
        except OSError:
            continue

    return None

# 분석 프롬프트
ANALYSIS_PROMPT = """
당신은 음식점 리뷰 분석 전문가입니다. 다음 리뷰를 **매우 세밀하게** 분석하세요.

**리뷰 내용:**
{review_content}
---

다음 JSON 형식으로 응답하세요:

{{
  "시설": {{
    "점수": -5에서 +5 사이 정수,
    "긍정키워드": ["분위기 좋음", "인테리어 힙함"],
    "부정키워드": ["좁음", "시끄러움"],
    "구체적언급": "시설 관련 언급 요약",
    "감정강도": 0~10 사이 정수
  }},
  "서비스": {{
    "점수": -5에서 +5 사이 정수,
    "긍정키워드": ["친절함", "빠른 응대"],
    "부정키워드": ["웨이팅 김", "불친절"],
    "구체적언급": "서비스 관련 언급 요약",
    "감정강도": 0~10 사이 정수
  }},
  "맛": {{
    "점수": -5에서 +5 사이 정수,
    "긍정키워드": ["맛있음", "신선함"],
    "부정키워드": ["짜요", "별로"],
    "구체적언급": "맛 관련 언급 요약",
    "감정강도": 0~10 사이 정수
  }},
  "메뉴평가": [
    {{
      "메뉴명": "원조닭전골",
      "평가": "긍정",
      "세부내용": "해당 메뉴 평가"
    }}
  ],
  "숨은불만": "미묘한 불만/개선점. 없으면 null",
  "고객니즈": "고객이 원하는 것",
  "개선제안": "구체적인 개선 제안 (없으면 null)",
  "재방문의도": "높음/중간/낮음/불명",
  "전체요약": "리뷰 핵심 1-2문장 요약"
}}

**규칙:**
1. 언급이 없는 항목은 점수 0, 키워드는 빈 리스트
2. 키워드는 최대 5개까지만
3. 반드시 유효한 JSON 형식으로 응답
4. 한국어로 응답
"""


class LLMReviewAnalyzer:
    """LLM 기반 리뷰 분석 클래스"""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """
        Args:
            api_key: OpenAI API 키 (None이면 환경변수에서 로드)
            model: 사용할 모델
        """
        load_dotenv()

        if not os.getenv("OPENAI_API_KEY"):
            load_env_from_shell_rc("OPENAI_API_KEY")
        if not os.getenv("OPENAI_MODEL"):
            load_env_from_shell_rc("OPENAI_MODEL")

        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError(
                "OPENAI_API_KEY가 설정되지 않았습니다! "
                "(.env 또는 ~/.zshrc에 설정하세요)"
            )

        self.model = model or os.getenv('OPENAI_MODEL', 'gpt-5-mini')
        self.client = OpenAI(api_key=self.api_key)

        # 병렬 처리용 락
        self.save_lock = threading.Lock()

        logger.info(f"LLM 분석기 초기화 완료 (모델: {self.model})")

    def analyze_single_review(self, review_content: str, max_retries: int = 2) -> Optional[Dict]:
        """
        단일 리뷰 분석

        Args:
            review_content: 리뷰 텍스트
            max_retries: 재시도 횟수

        Returns:
            분석 결과 딕셔너리
        """
        if not review_content or pd.isna(review_content):
            return None

        prompt = ANALYSIS_PROMPT.format(review_content=review_content)

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "당신은 고객 리뷰 분석 전문가입니다. 항상 유효한 JSON 형식으로 응답하세요."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    response_format={"type": "json_object"}
                )

                result = json.loads(response.choices[0].message.content)
                return result

            except Exception as e:
                logger.warning(f"분석 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(0.5)

        return None

    def _analyze_single_with_index(self, idx: int, review_content: str) -> tuple:
        """병렬 처리용 래퍼"""
        result = self.analyze_single_review(review_content)
        return idx, result

    def analyze_dataframe(
        self,
        df: pd.DataFrame,
        review_column: str = 'review',
        checkpoint_file: Optional[str] = None,
        max_workers: int = 10
    ) -> pd.DataFrame:
        """
        데이터프레임의 리뷰들을 병렬 분석

        Args:
            df: 리뷰가 포함된 데이터프레임
            review_column: 리뷰 텍스트가 있는 컬럼명
            checkpoint_file: 중간 저장 파일 경로
            max_workers: 병렬 작업 수

        Returns:
            분석 결과가 추가된 데이터프레임
        """
        # 기존 체크포인트 확인
        start_idx = 0
        if checkpoint_file and os.path.exists(checkpoint_file):
            logger.info(f"기존 체크포인트 발견: {checkpoint_file}")
            result_df = pd.read_csv(checkpoint_file)

            analyzed_count = result_df['시설_점수'].notna().sum()
            logger.info(f"이미 분석된 리뷰: {analyzed_count}건")

            if analyzed_count >= len(df):
                logger.info("모든 리뷰가 이미 분석되었습니다!")
                return result_df

            start_idx = analyzed_count
        else:
            result_df = df.copy()

            # 새 컬럼 추가
            new_cols = [
                '시설_점수', '시설_긍정키워드', '시설_부정키워드', '시설_언급', '시설_감정강도',
                '서비스_점수', '서비스_긍정키워드', '서비스_부정키워드', '서비스_언급', '서비스_감정강도',
                '맛_점수', '맛_긍정키워드', '맛_부정키워드', '맛_언급', '맛_감정강도',
                '메뉴평가_JSON', '숨은불만', '고객니즈', '개선제안', '재방문의도', '전체요약'
            ]
            for col in new_cols:
                result_df[col] = None

        total = len(df)
        success_count = 0
        fail_count = 0

        logger.info(f"병렬 분석 시작: {start_idx}번째부터 {total}번째까지")
        logger.info(f"병렬 작업 수: {max_workers}")
        logger.info(f"예상 소요 시간: 약 {(total - start_idx) * 1.5 / max_workers / 60:.1f}분")

        # 분석할 데이터 준비
        tasks = [(idx, df.iloc[idx][review_column]) for idx in range(start_idx, total)]

        # 병렬 실행
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._analyze_single_with_index, idx, content): idx
                for idx, content in tasks
            }

            with tqdm(total=len(tasks), desc="🔍 LLM 분석 진행") as pbar:
                for future in as_completed(futures):
                    idx, result = future.result()

                    if result:
                        try:
                            with self.save_lock:
                                self._save_analysis_to_df(result_df, idx, result)
                                success_count += 1
                        except Exception as e:
                            logger.error(f"결과 저장 실패 (idx={idx}): {e}")
                            fail_count += 1
                    else:
                        fail_count += 1

                    # 중간 저장 (100건마다)
                    if checkpoint_file and (success_count + fail_count) % 100 == 0:
                        with self.save_lock:
                            result_df.to_csv(checkpoint_file, index=False)
                            pbar.set_postfix({'성공': success_count, '실패': fail_count, '저장': '✓'})

                    pbar.update(1)

        # 최종 저장
        if checkpoint_file:
            result_df.to_csv(checkpoint_file, index=False)
            logger.info(f"최종 결과 저장: {checkpoint_file}")

        logger.info(f"분석 완료 - 성공: {success_count}건, 실패: {fail_count}건")

        return result_df

    def _save_analysis_to_df(self, df: pd.DataFrame, idx: int, result: Dict):
        """분석 결과를 데이터프레임에 저장"""
        # 시설
        df.loc[idx, '시설_점수'] = result.get('시설', {}).get('점수', 0)
        df.loc[idx, '시설_긍정키워드'] = ', '.join(result.get('시설', {}).get('긍정키워드', []))
        df.loc[idx, '시설_부정키워드'] = ', '.join(result.get('시설', {}).get('부정키워드', []))
        df.loc[idx, '시설_언급'] = result.get('시설', {}).get('구체적언급', '')
        df.loc[idx, '시설_감정강도'] = result.get('시설', {}).get('감정강도', 0)

        # 서비스
        df.loc[idx, '서비스_점수'] = result.get('서비스', {}).get('점수', 0)
        df.loc[idx, '서비스_긍정키워드'] = ', '.join(result.get('서비스', {}).get('긍정키워드', []))
        df.loc[idx, '서비스_부정키워드'] = ', '.join(result.get('서비스', {}).get('부정키워드', []))
        df.loc[idx, '서비스_언급'] = result.get('서비스', {}).get('구체적언급', '')
        df.loc[idx, '서비스_감정강도'] = result.get('서비스', {}).get('감정강도', 0)

        # 맛
        df.loc[idx, '맛_점수'] = result.get('맛', {}).get('점수', 0)
        df.loc[idx, '맛_긍정키워드'] = ', '.join(result.get('맛', {}).get('긍정키워드', []))
        df.loc[idx, '맛_부정키워드'] = ', '.join(result.get('맛', {}).get('부정키워드', []))
        df.loc[idx, '맛_언급'] = result.get('맛', {}).get('구체적언급', '')
        df.loc[idx, '맛_감정강도'] = result.get('맛', {}).get('감정강도', 0)

        # 추가 분석
        df.loc[idx, '메뉴평가_JSON'] = json.dumps(result.get('메뉴평가', []), ensure_ascii=False)
        df.loc[idx, '숨은불만'] = result.get('숨은불만', '')
        df.loc[idx, '고객니즈'] = result.get('고객니즈', '')
        df.loc[idx, '개선제안'] = result.get('개선제안', '')
        df.loc[idx, '재방문의도'] = result.get('재방문의도', '불명')
        df.loc[idx, '전체요약'] = result.get('전체요약', '')
