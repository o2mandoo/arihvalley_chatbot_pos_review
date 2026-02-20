import { resolveBackendConfig } from '@/lib/backend-url';

const SALES_FORCE_PREFIX = '매출 분석 질문: ';

function toTextChunks(text, chunkSize = 1, delayMs = 12) {
  const encoder = new TextEncoder();
  let offset = 0;

  return new ReadableStream({
    start(controller) {
      const pump = () => {
        if (offset >= text.length) {
          controller.close();
          return;
        }

        const next = text.slice(offset, offset + chunkSize);
        offset += chunkSize;
        controller.enqueue(encoder.encode(next));
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
        message: `${SALES_FORCE_PREFIX}${latestUser.content}`,
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
