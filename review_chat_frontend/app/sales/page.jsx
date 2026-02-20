'use client';

import { Children, cloneElement, isValidElement, useEffect, useMemo, useRef } from 'react';
import { useChat } from 'ai/react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const CHAT_STORAGE_KEY = 'sales-analyst:messages:v1';
const REVIEW_CHAT_URL = process.env.NEXT_PUBLIC_REVIEW_CHAT_URL?.trim() || '/';

const INITIAL_ASSISTANT_MESSAGE = {
  id: 'welcome-sales',
  role: 'assistant',
  content:
    '## 안녕하세요\n\n매출 흐름을 보기 쉽게 정리해드릴게요. 궁금한 내용을 편하게 질문해 주세요.',
};

const QUICK_PROMPTS = [
  '이번 달 일자별 매출 추이를 표로 보여줘',
  '전월 대비 매출 증감률을 알려줘',
  '지점별 매출 순위를 비교해줘',
  '객단가가 높은 시간대를 찾아줘',
];

const NEGATIVE_HIGHLIGHT_REGEX =
  /(웨이팅|대기|기다리|시끄럽|복잡|혼잡|늦|느리|오래\s*걸|불친절|별로|아쉽|좁|자리\s*없|비싸|가성비\s*별로|짜|싱겁|물리)/gi;

function stripLegacyHighlightTags(content) {
  return content
    .replace(/<span[^>]*class=["']neg-highlight["'][^>]*>/gi, '')
    .replace(/<\/span>/gi, '')
    .replace(/<strong>/gi, '')
    .replace(/<\/strong>/gi, '');
}

function highlightText(text, keyPrefix) {
  const parts = text.split(NEGATIVE_HIGHLIGHT_REGEX);
  if (parts.length <= 1) {
    return [text];
  }
  return parts.map((part, index) => {
    if (!part) return '';
    if (index % 2 === 1) {
      return (
        <span key={`${keyPrefix}-${index}`} className="neg-highlight">
          <strong>{part}</strong>
        </span>
      );
    }
    return part;
  });
}

function highlightInline(children, keyPrefix = 'neg') {
  return Children.toArray(children).flatMap((child, index) => {
    const key = `${keyPrefix}-${index}`;
    if (typeof child === 'string') {
      return highlightText(child, key);
    }
    if (!isValidElement(child)) {
      return child;
    }

    const typeName = typeof child.type === 'string' ? child.type : '';
    if (typeName === 'code' || typeName === 'pre') {
      return child;
    }

    if (child.props?.children) {
      return cloneElement(child, {
        ...child.props,
        children: highlightInline(child.props.children, key),
      });
    }
    return child;
  });
}

function MarkdownMessage({ content }) {
  const sanitized = stripLegacyHighlightTags(content);

  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        table: ({ ...props }) => <table className="md-table" {...props} />,
        th: ({ ...props }) => <th className="md-th" {...props} />,
        td: ({ ...props }) => <td className="md-td" {...props} />,
        p: ({ children, ...props }) => <p {...props}>{highlightInline(children, 'p')}</p>,
        li: ({ children, ...props }) => <li {...props}>{highlightInline(children, 'li')}</li>,
        pre: ({ children, ...props }) => {
          const child = Array.isArray(children) ? children[0] : children;
          const className = child?.props?.className || '';
          const isSqlBlock = typeof className === 'string' && className.includes('language-sql');

          if (isSqlBlock) {
            return (
              <details className="sql-toggle">
                <summary>분석 근거(SQL) 보기</summary>
                <pre className="md-pre" {...props}>
                  {children}
                </pre>
              </details>
            );
          }

          return (
            <pre className="md-pre" {...props}>
              {children}
            </pre>
          );
        },
        code: ({ ...props }) => <code className="md-code" {...props} />,
      }}
    >
      {sanitized}
    </ReactMarkdown>
  );
}

export default function SalesPage() {
  const messagesEndRef = useRef(null);

  const {
    messages,
    input,
    handleInputChange,
    handleSubmit,
    append,
    isLoading,
    status,
    error,
    setInput,
    setMessages,
    stop,
  } = useChat({
    api: '/api/sales-chat',
    streamProtocol: 'text',
    initialMessages: [INITIAL_ASSISTANT_MESSAGE],
  });

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isLoading]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(CHAT_STORAGE_KEY);
      if (!raw) return;

      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed) || parsed.length === 0) return;

      const restored = parsed
        .filter(
          (item) =>
            (item?.role === 'user' || item?.role === 'assistant') &&
            typeof item?.content === 'string',
        )
        .map((item, idx) => ({
          id: item?.id || `restored-sales-${idx}`,
          role: item.role,
          content: item.content,
        }));

      if (restored.length > 0) {
        setMessages(restored);
      }
    } catch (_) {
      // no-op
    }
  }, [setMessages]);

  useEffect(() => {
    try {
      const persisted = messages
        .filter((msg) => msg.role === 'user' || msg.role === 'assistant')
        .map((msg) => ({ id: msg.id, role: msg.role, content: msg.content }));
      localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(persisted));
    } catch (_) {
      // no-op
    }
  }, [messages]);

  const statusText = useMemo(() => {
    if (status === 'submitted' || status === 'streaming') return '답변을 준비하고 있어요.';
    if (status === 'error' || error) return '문제가 발생했어요.';
    return '질문을 기다리고 있어요.';
  }, [error, status]);

  const onQuickPrompt = async (text) => {
    setInput('');
    await append({ role: 'user', content: text });
  };

  const resetConversation = () => {
    setMessages([INITIAL_ASSISTANT_MESSAGE]);
    try {
      localStorage.removeItem(CHAT_STORAGE_KEY);
    } catch (_) {
      // no-op
    }
  };

  const copyToClipboard = async (text) => {
    try {
      await navigator.clipboard.writeText(text);
    } catch (_) {
      // no-op
    }
  };

  const submitByKeyboard = (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      if (!input.trim() || isLoading) {
        return;
      }
      handleSubmit(event);
    }
  };

  return (
    <main className="shell">
      <section className="panel sidebar">
        <h1>아리계곡 매출 도우미</h1>
        <p className="subtitle">매출 흐름과 비교 포인트를 한눈에 확인하세요.</p>

        <div className="card">
          <h2>리뷰 분석으로 이동</h2>
          <a className="switch-btn" href={REVIEW_CHAT_URL}>
            리뷰 분석 챗봇 열기
          </a>
          <p className="switch-note">고객 반응과 불만 패턴이 궁금하면 리뷰 분석으로 이동하세요.</p>
        </div>

        <div className="card">
          <h2>이런 질문이 좋아요</h2>
          {QUICK_PROMPTS.map((prompt) => (
            <button
              type="button"
              key={prompt}
              className="quick-btn"
              onClick={() => onQuickPrompt(prompt)}
            >
              {prompt}
            </button>
          ))}
        </div>
      </section>

      <section className="panel chatbox">
        <header className="chat-header">
          <div>
            <strong>매출 상담</strong>
            <p>{statusText}</p>
          </div>
          <div className="header-actions">
            {isLoading && (
              <button type="button" className="ghost-btn" onClick={stop}>
                응답 중지
              </button>
            )}
            <button type="button" className="ghost-btn" onClick={resetConversation}>
              새로 시작
            </button>
          </div>
        </header>

        <div className="messages">
          {messages.map((msg) => (
            <article
              key={msg.id}
              className={`bubble ${msg.role === 'user' ? 'user' : 'assistant'}`}
            >
              <div className="bubble-head">
                <span className="bubble-role">
                  {msg.role === 'user' ? '나' : '도우미'}
                </span>
                {msg.role === 'assistant' && (
                  <button
                    type="button"
                    className="copy-btn"
                    onClick={() => copyToClipboard(msg.content)}
                  >
                    복사
                  </button>
                )}
              </div>
              {msg.role === 'assistant' ? (
                <MarkdownMessage content={msg.content} />
              ) : (
                <p>{msg.content}</p>
              )}
            </article>
          ))}

          {isLoading && (
            <article className="bubble assistant loading">
              <span className="bubble-role">도우미</span>
              <p>답변을 작성하고 있어요...</p>
            </article>
          )}

          {error && (
            <article className="bubble assistant error">
              <span className="bubble-role">안내</span>
              <p>{error.message || '요청 처리 중 오류가 발생했어요. 잠시 후 다시 시도해 주세요.'}</p>
            </article>
          )}

          <div ref={messagesEndRef} />
        </div>

        <form
          className="composer"
          onSubmit={(event) => {
            event.preventDefault();
            handleSubmit(event);
          }}
        >
          <textarea
            name="prompt"
            rows={3}
            placeholder="예: 이번 달 지점별 매출 순위를 알려줘"
            value={input}
            onChange={handleInputChange}
            onKeyDown={submitByKeyboard}
            required
          />
          <button type="submit" disabled={isLoading || !input.trim()}>
            질문 보내기
          </button>
        </form>
      </section>
    </main>
  );
}
