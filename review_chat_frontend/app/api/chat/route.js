import { resolveBackendConfig } from '@/lib/backend-url';

const STREAM_OPTIONS = {
  minChunkSize: 14,
  maxChunkSize: 28,
  baseDelayMs: 1,
  sentencePauseMs: 6,
};

function toTextChunks(text, options = STREAM_OPTIONS) {
  const safeText = typeof text === 'string' ? text : '';
  const encoder = new TextEncoder();
  let offset = 0;
  const range = Math.max(0, options.maxChunkSize - options.minChunkSize);

  return new ReadableStream({
    start(controller) {
      const pump = () => {
        if (offset >= safeText.length) {
          controller.close();
          return;
        }

        const chunkSize =
          options.minChunkSize + (range > 0 ? Math.floor(Math.random() * (range + 1)) : 0);
        const end = Math.min(offset + chunkSize, safeText.length);
        const next = safeText.slice(offset, end);
        offset = end;
        controller.enqueue(encoder.encode(next));

        if (offset >= safeText.length) {
          controller.close();
          return;
        }

        const tail = next[next.length - 1] || '';
        const delayMs = /[.!?。！？\n]/.test(tail) ? options.sentencePauseMs : options.baseDelayMs;
        setTimeout(pump, delayMs);
      };

      pump();
    },
  });
}

export async function POST(req) {
  try {
    const body = await req.json();
    const messages = Array.isArray(body?.messages) ? body.messages : [];
    const latestUser = [...messages].reverse().find((msg) => msg.role === 'user');

    if (!latestUser?.content) {
      return new Response('질문이 비어 있습니다.', { status: 400 });
    }

    const { backendUrl } = resolveBackendConfig();

    const upstream = await fetch(`${backendUrl}/api/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: latestUser.content,
        history: messages,
      }),
      cache: 'no-store',
    });

    if (!upstream.ok) {
      const details = await upstream.text();
      return new Response(`백엔드 오류: ${upstream.status} ${details}`.trim(), {
        status: 502,
      });
    }

    const payload = await upstream.json();
    const markdown = payload.markdown || '응답이 비어 있습니다.';

    const stream = toTextChunks(markdown);

    return new Response(stream, {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
      },
    });
  } catch (error) {
    return new Response(`요청 처리 실패: ${error.message}`, { status: 500 });
  }
}
