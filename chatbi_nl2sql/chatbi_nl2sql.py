"""
ChatBI NL2SQL Agent - Vector DB 기반 Dynamic Few-shot 프롬프트를 활용한 NL2SQL 시스템

아키텍처:
1. 사용자 질문 입력
2. ChromaDB에서 유사한 질문-SQL 예시 검색 (Semantic Search)
3. 검색된 예시로 Dynamic Few-shot 프롬프트 생성
4. LLM이 SQL 생성
5. PostgreSQL에서 SQL 실행
6. 결과를 자연어로 변환하여 응답
"""

import os
import sys
import json
import pandas as pd
import re
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# LangChain imports
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_community.utilities import SQLDatabase
try:
    from langchain_classic.chains import create_sql_query_chain
except ImportError:  # pragma: no cover - fallback for older installs
    from langchain.chains import create_sql_query_chain
from sqlalchemy import create_engine


@dataclass
class FewShotExample:
    """Few-shot 예시 데이터 클래스"""
    question: str
    sql: str
    category: str


KOREAN_COLUMN_INFO = [
    ("레코드ID", "text", "레코드 고유 ID"),
    ("주문번호", "text", "주문 번호"),
    ("주문일자", "text", "주문 날짜 (YYYY-MM-DD)"),
    ("주문시간", "text", "주문 시간 (HH:mm:ss)"),
    ("지점명", "text", "지점명"),
    ("지역유형", "text", "지역 유형"),
    ("메뉴명", "text", "메뉴명"),
    ("카테고리", "text", "카테고리"),
    ("수량", "int", "주문 수량"),
    ("단가", "float", "단가"),
    ("실판매금액", "float", "실판매금액"),
    ("할인금액", "float", "할인 금액"),
    ("결제수단", "text", "결제 수단"),
    ("주문채널", "text", "주문 채널"),
    ("주문상태", "text", "주문 상태"),
    ("생성일시", "text", "레코드 생성 시각"),
    ("수정일시", "text", "레코드 수정 시각"),
]


class VectorStoreManager:
    """ChromaDB 기반 Vector Store 관리 클래스"""

    def __init__(self, persist_directory: str = "./chroma_db"):
        self.persist_directory = persist_directory
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self.vector_store = None

    def initialize_from_examples(self, examples: List[FewShotExample]) -> None:
        """Few-shot 예시로 Vector Store 초기화"""
        documents = []
        for i, example in enumerate(examples):
            # 질문을 Document로 변환, SQL은 metadata에 저장
            doc = Document(
                page_content=example.question,
                metadata={
                    "sql": example.sql,
                    "category": example.category,
                    "index": i
                }
            )
            documents.append(doc)

        # ChromaDB에 저장
        self.vector_store = Chroma.from_documents(
            documents=documents,
            embedding=self.embeddings,
            persist_directory=self.persist_directory,
            collection_name="nl2sql_examples"
        )
        print(f"✅ Vector Store 초기화 완료: {len(documents)}개 예시 저장")

    def load_existing(self) -> bool:
        """기존 Vector Store 로드"""
        try:
            self.vector_store = Chroma(
                persist_directory=self.persist_directory,
                embedding_function=self.embeddings,
                collection_name="nl2sql_examples"
            )
            count = self.vector_store._collection.count()
            if count > 0:
                print(f"✅ 기존 Vector Store 로드: {count}개 예시")
                return True
            return False
        except Exception:
            return False

    def search_similar(self, query: str, k: int = 5) -> List[Dict]:
        """유사한 질문-SQL 페어 검색"""
        if not self.vector_store:
            raise ValueError("Vector Store가 초기화되지 않았습니다.")

        results = self.vector_store.similarity_search_with_score(query, k=k)

        similar_examples = []
        for doc, score in results:
            similar_examples.append({
                "question": doc.page_content,
                "sql": doc.metadata["sql"],
                "category": doc.metadata["category"],
                "similarity_score": 1 - score  # distance를 similarity로 변환
            })

        return similar_examples


def load_env_from_shell_rc(var_name: str) -> Optional[str]:
    """zsh rc 파일에서 환경변수를 찾고, 설정되어 있으면 반환"""
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


def normalize_sql(sql: str) -> str:
    """SQL 문자열 정제 (코드블록/프리픽스 제거)"""
    sql = sql.strip()
    if "```" in sql:
        parts = sql.split("```")
        if len(parts) >= 2:
            sql = parts[1]
    if sql.startswith("```"):
        sql = sql.split("\n", 1)[1] if "\n" in sql else sql[3:]
    if sql.endswith("```"):
        sql = sql.rsplit("```", 1)[0]
    sql = sql.strip()
    if sql.lower().startswith("sql"):
        sql = sql.split("\n", 1)[1] if "\n" in sql else sql[3:]
        sql = sql.strip()

    upper = sql.upper()
    for prefix in ("SQLQUERY:", "SQL:"):
        if upper.startswith(prefix):
            sql = sql.split(":", 1)[1].strip()
            break
    return sql


class PostgresSQLExecutor:
    """PostgreSQL 기반 SQL 실행기"""

    def __init__(self, db_url: str, table_name: str = "sales_records"):
        self.db_url = db_url
        self.table_name = table_name
        self.engine = create_engine(db_url, pool_pre_ping=True)

    def load_dataframe(self, df: pd.DataFrame, table_name: str = "sales") -> None:
        raise NotImplementedError("PostgreSQL 모드에서는 엑셀 로드 기능을 지원하지 않습니다.")

    def get_schema(self, table_name: Optional[str] = None) -> str:
        table = table_name or self.table_name
        schema_info = [f'  - "{name}" ({dtype})' for name, dtype, _ in KOREAN_COLUMN_INFO]
        return f"테이블명: {table}\n컬럼:\n" + "\n".join(schema_info)

    def get_sample_data(self, table_name: Optional[str] = None, limit: int = 3) -> str:
        table = table_name or self.table_name
        columns = ", ".join([f'"{name}"' for name, _, _ in KOREAN_COLUMN_INFO])
        df = pd.read_sql_query(f'SELECT {columns} FROM {table} LIMIT {limit}', self.engine)
        return df.to_string(index=False)

    def execute_sql(self, sql: str) -> Tuple[bool, any, str]:
        try:
            sql = normalize_sql(sql)
            df = pd.read_sql_query(sql, self.engine)
            return True, df, ""
        except Exception as e:
            return False, None, str(e)

    def close(self):
        if self.engine:
            self.engine.dispose()


class NL2SQLAgent:
    """NL2SQL 에이전트 메인 클래스"""

    def __init__(
        self,
        model_name: Optional[str] = None,
        vector_store_path: str = "./chroma_db",
        num_examples: int = 5,
        db_url: Optional[str] = None,
        table_name: str = "sales_records",
        use_langchain_sql: bool = False,
        use_few_shot: bool = True
    ):
        self.num_examples = num_examples
        self.db_url = db_url
        self.table_name = table_name
        self.use_langchain_sql = use_langchain_sql
        self.use_few_shot = use_few_shot
        self.sql_dialect = "PostgreSQL"
        self.langchain_sql_chain = None
        self.sql_database = None

        # OpenAI API 키 확인
        if not os.environ.get("OPENAI_API_KEY"):
            load_env_from_shell_rc("OPENAI_API_KEY")
        if not os.environ.get("OPENAI_MODEL"):
            load_env_from_shell_rc("OPENAI_MODEL")

        self.model_name = model_name or os.environ.get("OPENAI_MODEL", "gpt-5-mini")

        if not os.environ.get("OPENAI_API_KEY"):
            raise EnvironmentError(
                "OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.\n"
                "export OPENAI_API_KEY='your-api-key'"
            )

        # LLM 초기화
        self.llm = ChatOpenAI(model=self.model_name, temperature=0)

        # Vector Store 초기화
        self.vector_store_manager = VectorStoreManager(vector_store_path) if self.use_few_shot else None

        if not self.db_url:
            raise EnvironmentError(
                "DATABASE_URL 환경 변수가 설정되지 않았습니다.\n"
                "export DATABASE_URL='postgresql://user:pass@localhost:5433/dbname'"
            )

        # SQL 실행기
        self.sql_executor = PostgresSQLExecutor(self.db_url, table_name=self.table_name)

        # 테이블 스키마 (데이터 로드 후 설정)
        self.table_schema = ""
        self.sample_data = ""
        self.last_sql: Optional[str] = None
        self.last_result_df: Optional[pd.DataFrame] = None
        self.last_mode: Optional[str] = None

        if self.db_url and self.use_langchain_sql:
            table_info = self._build_korean_table_info()
            try:
                self.sql_database = SQLDatabase.from_uri(
                    self.db_url,
                    include_tables=[self.table_name],
                    sample_rows_in_table_info=2,
                    custom_table_info={self.table_name: table_info},
                    view_support=True,
                )
            except TypeError:
                self.sql_database = SQLDatabase.from_uri(
                    self.db_url,
                    include_tables=[self.table_name],
                    sample_rows_in_table_info=2,
                    view_support=True,
                )
            self.langchain_sql_chain = create_sql_query_chain(self.llm, self.sql_database)

    def load_few_shot_examples(self, examples_path: str) -> None:
        """JSON 파일에서 Few-shot 예시 로드"""
        if not self.use_few_shot or not self.vector_store_manager:
            return
        # 기존 Vector Store가 있으면 로드
        if self.vector_store_manager.load_existing():
            return

        # 없으면 새로 생성
        with open(examples_path, 'r', encoding='utf-8') as f:
            examples_data = json.load(f)

        examples = [
            FewShotExample(
                question=ex["question"],
                sql=ex["sql"],
                category=ex["category"]
            )
            for ex in examples_data
        ]

        self.vector_store_manager.initialize_from_examples(examples)

    def load_db_context(self) -> None:
        """PostgreSQL에서 스키마/샘플 데이터 로드"""
        self.table_schema = self.sql_executor.get_schema(self.table_name)
        self.sample_data = self.sql_executor.get_sample_data(self.table_name)
        print(f"\n📊 데이터 정보:")
        print(self.table_schema)

    def _build_korean_table_info(self) -> str:
        """LangChain SQL 체인용 한글 컬럼 스키마 문자열 생성"""
        columns = ",\n  ".join([f'"{name}" {dtype}' for name, dtype, _ in KOREAN_COLUMN_INFO])
        return (
            "-- 한글 컬럼명은 반드시 쌍따옴표로 감싸서 사용하세요.\n"
            f"CREATE TABLE {self.table_name} (\n  {columns}\n);"
        )

    def _build_prompt(self, question: str, similar_examples: List[Dict]) -> str:
        """Dynamic Few-shot 프롬프트 생성"""

        # Few-shot 예시 문자열 생성
        examples_str = ""
        for i, ex in enumerate(similar_examples, 1):
            examples_str += f"""
예시 {i}:
질문: {ex['question']}
SQL: {ex['sql']}
"""

        rules = f"""1. PostgreSQL 문법을 사용하세요.
2. 날짜 함수는 CURRENT_DATE, DATE_TRUNC(), EXTRACT() 등을 사용하세요.
3. "주문일자"는 텍스트이므로 날짜 비교 시 TO_DATE("주문일자", 'YYYY-MM-DD')로 변환하세요.
4. "주문 건수"는 COUNT(DISTINCT ("주문번호", "주문일자"))로 집계하세요.
5. "시간대별"은 EXTRACT(HOUR FROM "주문시간"::time)::int로 집계하세요.
6. 한글 컬럼명은 반드시 쌍따옴표로 감싸서 사용하세요.
7. 반드시 SQL만 출력하세요. 설명이나 마크다운 코드 블록 없이 순수 SQL만 작성하세요.
8. 테이블명은 '{self.table_name}'입니다."""

        prompt = f"""당신은 자연어를 SQL로 변환하는 전문가입니다.
주어진 데이터베이스 스키마와 유사한 질문-SQL 예시를 참고하여, 사용자의 질문에 맞는 SQL을 생성하세요.

## 데이터베이스 스키마
{self.table_schema}

## 샘플 데이터
{self.sample_data}

## 유사한 질문-SQL 예시 (참고용)
{examples_str}

## 규칙
{rules}

## 사용자 질문
{question}

## SQL:
"""
        return prompt

    def _format_result(self, question: str, sql: str, result_df: pd.DataFrame) -> str:
        """결과를 자연어로 포맷팅"""

        # 결과 요약을 위한 프롬프트
        result_str = result_df.to_string(index=False) if len(result_df) <= 20 else result_df.head(20).to_string(index=False) + f"\n... (총 {len(result_df)}행)"

        format_prompt = f"""다음은 사용자 질문에 대한 SQL 쿼리 결과입니다.
결과를 한국어로 친절하게 설명해주세요. 숫자는 천 단위 구분자(,)를 사용하세요.

질문: {question}

실행된 SQL:
{sql}

쿼리 결과:
{result_str}

위 결과를 바탕으로 사용자에게 답변해주세요:"""

        response = self.llm.invoke(format_prompt)
        return response.content

    def _is_simple_question(self, question: str) -> bool:
        """간단 질의 여부 판단 (LangChain SQL 우선 적용용)"""
        complex_markers = [
            "비율", "추이", "증감", "상관", "통계", "비교", "조합", "프로모션",
            "상위", "하위", "top", "rank", "유의미", "분석", "연관"
        ]
        if any(marker in question.lower() for marker in complex_markers):
            return False
        return len(question) <= 60

    def _extract_sql_output(self, output: any) -> str:
        if isinstance(output, dict):
            for key in ("query", "sql", "result", "output"):
                if key in output:
                    return str(output[key])
            return json.dumps(output, ensure_ascii=False)
        return str(output)

    def _rewrite_date_filters(self, sql: str) -> str:
        if 'TO_DATE("주문일자"' in sql:
            return sql
        pattern = r'"주문일자"\s*(=|<>|!=|>=|<=|>|<|BETWEEN)\s*'
        return re.sub(
            pattern,
            r'TO_DATE("주문일자", \'YYYY-MM-DD\') \1 ',
            sql,
            flags=re.IGNORECASE
        )

    def _rewrite_order_counts(self, sql: str) -> str:
        if 'DISTINCT ("주문번호", "주문일자")' in sql:
            return sql
        sql = re.sub(
            r'COUNT\s*\(\s*"주문번호"\s*\)',
            'COUNT(DISTINCT ("주문번호", "주문일자"))',
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r'COUNT\s*\(\s*\*\s*\)',
            'COUNT(DISTINCT ("주문번호", "주문일자"))',
            sql,
            flags=re.IGNORECASE
        )
        return sql

    def _rewrite_time_buckets(self, sql: str) -> str:
        if "EXTRACT(HOUR" in sql.upper():
            return sql
        sql = re.sub(
            r'SELECT\s+"주문시간"\b',
            'SELECT EXTRACT(HOUR FROM "주문시간"::time)::int as hour',
            sql,
            flags=re.IGNORECASE
        )
        return re.sub(
            r'"주문시간"\b',
            'EXTRACT(HOUR FROM "주문시간"::time)::int',
            sql,
            flags=re.IGNORECASE
        )

    def _post_process_sql(self, question: str, sql: str) -> str:
        fixed = normalize_sql(sql)
        fixed = self._rewrite_date_filters(fixed)

        order_markers = ["주문 건수", "주문건수", "주문 수", "주문수", "주문 개수", "주문개수", "주문건"]
        if any(marker in question for marker in order_markers):
            fixed = self._rewrite_order_counts(fixed)

        time_markers = ["시간대", "시간대별", "시간별", "시간 분포", "시간대 분포"]
        if any(marker in question for marker in time_markers):
            fixed = self._rewrite_time_buckets(fixed)

        return fixed

    def _enhance_question_for_sql_chain(self, question: str) -> str:
        hints = []
        hints.append('날짜 비교는 TO_DATE("주문일자", \'YYYY-MM-DD\')로 변환해서 비교하세요.')

        order_markers = ["주문 건수", "주문건수", "주문 수", "주문수", "주문 개수", "주문개수", "주문건"]
        if any(marker in question for marker in order_markers):
            hints.append('주문 건수는 COUNT(DISTINCT ("주문번호", "주문일자"))로 집계하세요.')

        time_markers = ["시간대", "시간대별", "시간별", "시간 분포", "시간대 분포"]
        if any(marker in question for marker in time_markers):
            hints.append('시간대별 집계는 EXTRACT(HOUR FROM "주문시간"::time)::int 기준으로 그룹핑하세요.')

        if not hints:
            return question

        return question + "\n\nSQL 작성 힌트: " + " ".join(hints)

    def _generate_sql_langchain(self, question: str) -> str:
        if not self.langchain_sql_chain:
            raise ValueError("LangChain SQL 체인이 초기화되지 않았습니다.")
        enhanced_question = self._enhance_question_for_sql_chain(question)
        try:
            output = self.langchain_sql_chain.invoke({"question": enhanced_question})
        except Exception:
            output = self.langchain_sql_chain.invoke({"input": enhanced_question})
        sql = self._extract_sql_output(output)
        return normalize_sql(sql)

    def _execute_with_retry(self, question: str, sql: str) -> Tuple[bool, Optional[pd.DataFrame], str, str]:
        sql = self._post_process_sql(question, sql)
        success, result_df, error = self.sql_executor.execute_sql(sql)
        if success:
            return True, result_df, sql, ""

        retry_prompt = f"""이전 SQL 실행 중 오류가 발생했습니다.

오류 메시지: {error}

원래 SQL:
{sql}

스키마:
{self.table_schema}

오류를 수정하여 올바른 SQL을 작성해주세요. SQL만 출력하세요:"""

        retry_response = self.llm.invoke(retry_prompt)
        fixed_sql = retry_response.content.strip()
        fixed_sql = self._post_process_sql(question, fixed_sql)
        success, result_df, error = self.sql_executor.execute_sql(fixed_sql)
        if success:
            return True, result_df, fixed_sql, ""

        return False, None, fixed_sql, error

    def format_result_table(self, result_df: Optional[pd.DataFrame], limit: int = 20) -> str:
        """결과를 터미널에서 볼 수 있는 표 형태로 출력"""
        if result_df is None or result_df.empty:
            return ""
        preview_df = result_df.copy()

        columns_env = os.environ.get("CHATBI_PREVIEW_COLUMNS", "")
        if columns_env:
            columns = [col.strip() for col in columns_env.split(",") if col.strip()]
            existing = [col for col in columns if col in preview_df.columns]
            if existing:
                preview_df = preview_df[existing]

        sort_env = os.environ.get("CHATBI_PREVIEW_SORT", "").strip()
        if sort_env:
            sort_col = sort_env
            sort_order = "asc"
            if ":" in sort_env:
                sort_col, sort_order = [part.strip() for part in sort_env.split(":", 1)]
            elif " " in sort_env:
                parts = [part.strip() for part in sort_env.split(" ", 1)]
                sort_col = parts[0]
                if len(parts) > 1:
                    sort_order = parts[1]

            if sort_col in preview_df.columns:
                ascending = sort_order.lower() != "desc"
                preview_df = preview_df.sort_values(by=sort_col, ascending=ascending)

        limit_env = os.environ.get("CHATBI_PREVIEW_LIMIT", "").strip()
        preview_limit = limit
        if limit_env.isdigit():
            preview_limit = max(1, int(limit_env))

        if len(preview_df) <= preview_limit:
            return preview_df.to_string(index=False)
        head = preview_df.head(preview_limit).to_string(index=False)
        return f"{head}\n... (총 {len(preview_df)}행)"

    def query(self, question: str) -> str:
        """사용자 질문 처리"""
        langchain_attempted = False

        if self.use_langchain_sql and self._is_simple_question(question):
            langchain_attempted = True
            self.last_mode = "langchain"
            print("\n🤖 (LangChain SQL) 쉬운 질의 처리 중...")
            generated_sql = self._generate_sql_langchain(question)
            print(f"\n📝 생성된 SQL:\n{generated_sql}")

            print("\n⚙️ SQL 실행 중...")
            success, result_df, final_sql, error = self._execute_with_retry(question, generated_sql)
            if success:
                self.last_sql = final_sql
                self.last_result_df = result_df
                print("\n📊 결과 분석 중...")
                return self._format_result(question, final_sql, result_df)
            print(f"⚠️ LangChain SQL 실행 오류: {error}")

        if self.use_few_shot and self.vector_store_manager:
            self.last_mode = "few_shot"
            print("\n🔍 유사한 예시 검색 중...")
            similar_examples = self.vector_store_manager.search_similar(
                question,
                k=self.num_examples
            )

            print(f"   검색된 예시 {len(similar_examples)}개:")
            for i, ex in enumerate(similar_examples[:3], 1):
                print(f"   {i}. {ex['question'][:40]}... (유사도: {ex['similarity_score']:.3f})")

            print("\n🤖 SQL 생성 중...")
            prompt = self._build_prompt(question, similar_examples)
            sql_response = self.llm.invoke(prompt)
            generated_sql = sql_response.content.strip()

            print(f"\n📝 생성된 SQL:\n{generated_sql}")
            print("\n⚙️ SQL 실행 중...")
            success, result_df, final_sql, error = self._execute_with_retry(question, generated_sql)

            if success:
                self.last_sql = final_sql
                self.last_result_df = result_df
                print("\n📊 결과 분석 중...")
                return self._format_result(question, final_sql, result_df)

            print(f"⚠️ Few-shot SQL 실행 오류: {error}")

        if self.use_langchain_sql and not langchain_attempted:
            self.last_mode = "langchain"
            print("\n🤖 (LangChain SQL) 폴백 실행 중...")
            generated_sql = self._generate_sql_langchain(question)
            print(f"\n📝 생성된 SQL:\n{generated_sql}")

            print("\n⚙️ SQL 실행 중...")
            success, result_df, final_sql, error = self._execute_with_retry(question, generated_sql)
            if success:
                self.last_sql = final_sql
                self.last_result_df = result_df
                print("\n📊 결과 분석 중...")
                return self._format_result(question, final_sql, result_df)
            self.last_sql = final_sql
            self.last_result_df = None
            return f"❌ SQL 실행 실패: {error}\n\n생성된 SQL:\n{final_sql}"

        return "❌ SQL 생성 전략이 초기화되지 않았습니다."

    def close(self):
        """리소스 정리"""
        self.sql_executor.close()


def print_header():
    """헤더 출력"""
    print("\n" + "=" * 60)
    print("🚀 ChatBI NL2SQL Agent (Postgres/LangChain 지원)")
    print("=" * 60)


def run_chat_loop(agent: NL2SQLAgent):
    """대화 루프 실행"""
    print("\n💬 ChatBI가 준비되었습니다!")
    print("매출 데이터에 대해 자연어로 질문해보세요.")
    print("-" * 60)
    print("예시 질문:")
    print("  • 오늘 전체 지점 총 매출 합계 얼마야?")
    print("  • 가장 많이 팔린 메뉴 Top 5 알려줘")
    print("  • 지점별 매출을 비교해줘")
    print("-" * 60)
    print("(종료: 'exit' 입력)\n")

    while True:
        try:
            user_input = input("질문을 입력하세요 (종료: exit): ").strip()

            if user_input.lower() in ['exit', 'quit', '종료']:
                print("\n👋 ChatBI를 종료합니다. 감사합니다!")
                break

            if not user_input:
                print("⚠️ 질문을 입력해주세요.\n")
                continue

            # 질문 처리
            answer = agent.query(user_input)

            print("\n" + "=" * 60)
            print("📝 답변:")
            print("=" * 60)
            print(answer)
            if agent.last_sql:
                print("\n" + "-" * 60)
                print("🔎 실행된 SQL:")
                print("-" * 60)
                print(agent.last_sql)
            result_table = agent.format_result_table(agent.last_result_df)
            if result_table:
                print("\n" + "-" * 60)
                print("📋 결과 미리보기:")
                print("-" * 60)
                print(result_table)
            print("=" * 60 + "\n")

        except KeyboardInterrupt:
            print("\n\n👋 ChatBI를 종료합니다. 감사합니다!")
            break
        except Exception as e:
            print(f"\n❌ 오류 발생: {str(e)}")
            print("다른 질문을 시도해보세요.\n")


def main():
    """메인 함수"""
    # 기본 설정
    EXAMPLES_FILE = "chatbi_nl2sql/few_shot_examples.json"
    EXAMPLES_FILE_PG = "chatbi_nl2sql/few_shot_examples_postgres_ko.json"
    VECTOR_STORE_PATH = "./chatbi_nl2sql/chroma_db"
    if not os.environ.get("OPENAI_MODEL"):
        load_env_from_shell_rc("OPENAI_MODEL")
    if not os.environ.get("DATABASE_URL"):
        load_env_from_shell_rc("DATABASE_URL")

    DATABASE_URL = os.environ.get("DATABASE_URL")
    MODEL_NAME = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
    TABLE_NAME = os.environ.get("CHATBI_TABLE", "sales_records")
    USE_FEW_SHOT = os.environ.get("CHATBI_USE_FEW_SHOT", "0") == "1"

    print_header()
    if not DATABASE_URL:
        raise EnvironmentError(
            "DATABASE_URL 환경 변수가 설정되지 않았습니다.\n"
            "export DATABASE_URL='postgresql://user:pass@localhost:5433/dbname'"
        )

    print(f"🗄️  DB: {DATABASE_URL}")
    print(f"📋 테이블: {TABLE_NAME}")
    print(f"🤖 모델: {MODEL_NAME}")
    print(f"📚 Few-shot 사용: {USE_FEW_SHOT}")

    try:
        # 에이전트 초기화
        print("\n🔧 에이전트 초기화 중...")
        if DATABASE_URL and os.path.exists(EXAMPLES_FILE_PG):
            EXAMPLES_FILE = EXAMPLES_FILE_PG
            VECTOR_STORE_PATH = "./chatbi_nl2sql/chroma_db_pg_ko"

        agent = NL2SQLAgent(
            model_name=MODEL_NAME,
            vector_store_path=VECTOR_STORE_PATH,
            num_examples=5,
            db_url=DATABASE_URL,
            table_name=TABLE_NAME,
            use_langchain_sql=bool(DATABASE_URL),
            use_few_shot=USE_FEW_SHOT if DATABASE_URL else True
        )
        if USE_FEW_SHOT:
            print("\n📚 Few-shot 예시 로드 중...")
            agent.load_few_shot_examples(EXAMPLES_FILE)

        print("\n📂 DB 스키마 로드 중...")
        agent.load_db_context()

        # 대화 루프 실행
        run_chat_loop(agent)

        # 정리
        agent.close()

    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        sys.exit(1)
    except EnvironmentError as e:
        print(f"\n❌ {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
