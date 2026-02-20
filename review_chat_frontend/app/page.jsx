'use client';

import { useEffect, useMemo, useRef } from 'react';
import { useChat } from 'ai/react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const CHAT_STORAGE_KEY = 'review-analyst:messages:v1';
const SALES_CHAT_URL = process.env.NEXT_PUBLIC_SALES_CHAT_URL?.trim();
const INITIAL_ASSISTANT_MESSAGE = {
  id: 'welcome',
  role: 'assistant',
  content:
    '## 안녕하세요\n\n매장 리뷰에서 고객 반응을 쉽게 파악해드릴게요. 궁금한 내용을 편하게 질문해 주세요.',
};

const QUICK_PROMPTS = [
  '최근 리뷰에서 자주 반복되는 아쉬운 점을 알려줘',
  '칭찬 리뷰 안에 숨어 있는 불만 사례를 보여줘',
  '웨이팅 관련 불만을 보기 쉽게 표로 정리해줘',
  '지점별로 가장 많이 언급된 불만을 비교해줘',
];

function MarkdownMessage({ content }) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        table: ({ ...props }) => <table className="md-table" {...props} />,
        th: ({ ...props }) => <th className="md-th" {...props} />,
        td: ({ ...props }) => <td className="md-td" {...props} />,
        pre: ({ ...props }) => <pre className="md-pre" {...props} />,
        code: ({ ...props }) => <code className="md-code" {...props} />,
      }}
    >
      {content}
    </ReactMarkdown>
  );
}

export default function HomePage() {
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
    api: '/api/chat',
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
          id: item?.id || `restored-${idx}`,
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
        <h1>아리계곡 리뷰 도우미</h1>
        <p className="subtitle">리뷰를 쉽게 읽고, 중요한 포인트만 빠르게 확인하세요.</p>

        <div className="card">
          <h2>매출 분석으로 이동</h2>
          {SALES_CHAT_URL ? (
            <a className="switch-btn" href={SALES_CHAT_URL}>
              매출 분석 챗봇 열기
            </a>
          ) : (
            <button type="button" className="switch-btn disabled" disabled>
              매출 분석 챗봇 준비 중
            </button>
          )}
          <p className="switch-note">리뷰가 아닌 매출 질문은 이 버튼으로 이동해서 물어보세요.</p>
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
            <strong>리뷰 상담</strong>
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
            placeholder="예: 최근 리뷰에서 반복적으로 아쉬운 점을 알려줘"
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
