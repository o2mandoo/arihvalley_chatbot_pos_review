'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useChat } from 'ai/react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const CHAT_STORAGE_KEY = 'review-analyst:messages:v1';
const INITIAL_ASSISTANT_MESSAGE = {
  id: 'welcome',
  role: 'assistant',
  content:
    '## 리뷰 분석 챗봇\n\n질문을 입력하면 리뷰 데이터 text2SQL + 반복 부정 신호 해석을 수행합니다.',
};

const QUICK_PROMPTS = [
  '강남점 리뷰에서 반복되는 부정 신호 Top 5를 보여줘',
  '긍정 리뷰 속 숨은 불만 사례를 5개 보여줘',
  '종각점 리뷰에서 웨이팅 관련 불만을 표로 정리해줘',
  '매출 분석 해줘',
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
  const [runtimeInfo, setRuntimeInfo] = useState(null);

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
    let cancelled = false;

    const loadRuntimeInfo = async () => {
      try {
        const response = await fetch('/api/runtime', { cache: 'no-store' });
        if (!response.ok) return;
        const payload = await response.json();
        if (!cancelled) {
          setRuntimeInfo(payload);
        }
      } catch (_) {
        // no-op
      }
    };

    loadRuntimeInfo();

    return () => {
      cancelled = true;
    };
  }, []);

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
    if (status === 'submitted' || status === 'streaming') return '분석 중...';
    if (status === 'error' || error) return '오류 발생';
    return 'Ready';
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
        <h1>Review Analyst</h1>
        <p className="subtitle">Next.js + @vercel/ai + Markdown UI</p>

        <div className="card">
          <h2>Data Source</h2>
          <code>review_analysis/data/아리계곡_통합_.csv</code>
        </div>

        <div className="card">
          <h2>Runtime Check</h2>
          <p className="runtime-line">
            env: <code>{runtimeInfo?.deploymentEnv || 'unknown'}</code>
          </p>
          <p className="runtime-line">
            vercelEnv: <code>{runtimeInfo?.vercelEnv || 'local'}</code>
          </p>
          <p className="runtime-line">
            nodeEnv: <code>{runtimeInfo?.nodeEnv || 'unknown'}</code>
          </p>
          <p className="runtime-line">
            source: <code>{runtimeInfo?.source || 'unknown'}</code>
          </p>
          <p className="runtime-line">
            backend: <code>{runtimeInfo?.backendUrl || 'unknown'}</code>
          </p>
        </div>

        <div className="card">
          <h2>Quick Prompts</h2>
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
            <strong>Chat</strong>
            <p>{statusText}</p>
          </div>
          <div className="header-actions">
            {isLoading && (
              <button type="button" className="ghost-btn" onClick={stop}>
                중단
              </button>
            )}
            <button type="button" className="ghost-btn" onClick={resetConversation}>
              새 대화
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
                  {msg.role === 'user' ? 'You' : 'Assistant'}
                </span>
                {msg.role === 'assistant' && (
                  <button
                    type="button"
                    className="copy-btn"
                    onClick={() => copyToClipboard(msg.content)}
                  >
                    Copy
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
              <span className="bubble-role">Assistant</span>
              <p>응답 생성 중...</p>
            </article>
          )}

          {error && (
            <article className="bubble assistant error">
              <span className="bubble-role">Error</span>
              <p>{error.message}</p>
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
            placeholder="리뷰 분석 질문을 입력하세요..."
            value={input}
            onChange={handleInputChange}
            onKeyDown={submitByKeyboard}
            required
          />
          <button type="submit" disabled={isLoading || !input.trim()}>
            Send
          </button>
        </form>
      </section>
    </main>
  );
}
