"""
설정 파일 모듈
"""
from .config import (
    STORES,
    CRAWLING_CONFIG,
    get_review_filepath,
    get_analysis_filepath,
    DATA_DIR,
    RESULTS_DIR
)

__all__ = [
    'STORES',
    'CRAWLING_CONFIG',
    'get_review_filepath',
    'get_analysis_filepath',
    'DATA_DIR',
    'RESULTS_DIR'
]
