import { resolveBackendConfig } from '@/lib/backend-url';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET() {
  try {
    const { backendUrl } = resolveBackendConfig();
    const upstream = await fetch(`${backendUrl}/api/sales/source`, {
      method: 'GET',
      cache: 'no-store',
    });

    const text = await upstream.text();
    if (!upstream.ok) {
      return new Response(text || '매출 소스 조회 실패', { status: upstream.status });
    }

    return new Response(text, {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  } catch (error) {
    return new Response(JSON.stringify({ detail: `요청 처리 실패: ${error.message}` }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
      },
    });
  }
}

export async function POST(req) {
  try {
    const payload = await req.json();
    const { backendUrl } = resolveBackendConfig();

    const upstream = await fetch(`${backendUrl}/api/sales/source`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {}),
      cache: 'no-store',
    });

    const text = await upstream.text();
    if (!upstream.ok) {
      return new Response(text || '매출 소스 업데이트 실패', { status: upstream.status });
    }

    return new Response(text, {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  } catch (error) {
    return new Response(JSON.stringify({ detail: `요청 처리 실패: ${error.message}` }), {
      status: 500,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
      },
    });
  }
}
