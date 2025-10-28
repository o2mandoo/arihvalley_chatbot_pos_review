"""
데이터 관리 모듈 - 증분 업데이트 및 중복 제거
"""
import pandas as pd
import hashlib
from pathlib import Path
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


class ReviewDataManager:
    """리뷰 데이터 관리 클래스"""

    def __init__(self, data_filepath: str):
        """
        Args:
            data_filepath: 리뷰 데이터 CSV 파일 경로
        """
        self.filepath = Path(data_filepath)
        self.df = None

    def load_data(self) -> pd.DataFrame:
        """기존 데이터 로드"""
        if self.filepath.exists():
            self.df = pd.read_csv(self.filepath)
            logger.info(f"기존 데이터 로드: {len(self.df)}건")
        else:
            self.df = pd.DataFrame(columns=['review', 'review_hash'])
            logger.info("새로운 데이터프레임 생성")
        return self.df

    @staticmethod
    def generate_hash(text: str) -> str:
        """텍스트의 해시값 생성"""
        if pd.isna(text):
            return None
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def add_reviews(self, new_reviews: List[str]) -> Tuple[pd.DataFrame, int]:
        """
        새로운 리뷰 추가 (중복 제거)

        Args:
            new_reviews: 새로운 리뷰 텍스트 리스트

        Returns:
            (업데이트된 DataFrame, 추가된 리뷰 개수)
        """
        # 기존 데이터 로드
        if self.df is None:
            self.load_data()

        # 해시값 생성
        new_df = pd.DataFrame({'review': new_reviews})
        new_df['review_hash'] = new_df['review'].apply(self.generate_hash)

        # 기존 해시값 집합
        existing_hashes = set(self.df['review_hash'].dropna())

        # 중복 제거
        new_df = new_df[~new_df['review_hash'].isin(existing_hashes)]

        added_count = len(new_df)

        if added_count > 0:
            # 데이터 추가
            self.df = pd.concat([self.df, new_df], ignore_index=True)
            logger.info(f"새로운 리뷰 {added_count}건 추가")
        else:
            logger.info("중복 제거 후 추가할 리뷰 없음")

        return self.df, added_count

    def save_data(self) -> bool:
        """데이터 저장"""
        try:
            if self.df is not None:
                self.df.to_csv(self.filepath, index=False, encoding='utf-8-sig')
                logger.info(f"데이터 저장 완료: {self.filepath}")
                return True
        except Exception as e:
            logger.error(f"데이터 저장 실패: {e}")
            return False

    def merge_and_update(self, new_reviews: List[str]) -> int:
        """
        새로운 리뷰를 병합하고 저장 (원스톱)

        Args:
            new_reviews: 새로운 리뷰 텍스트 리스트

        Returns:
            추가된 리뷰 개수
        """
        _, added_count = self.add_reviews(new_reviews)
        if added_count > 0:
            self.save_data()
        return added_count

    def get_statistics(self) -> dict:
        """데이터 통계 반환"""
        if self.df is None:
            self.load_data()

        return {
            'total_reviews': len(self.df),
            'unique_reviews': self.df['review_hash'].nunique(),
            'average_length': self.df['review'].str.len().mean() if len(self.df) > 0 else 0
        }
