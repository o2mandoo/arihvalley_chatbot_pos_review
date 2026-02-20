import { resolveBackendConfig } from '@/lib/backend-url';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET() {
  const { backendUrl, source, vercelEnv, nodeEnv, deploymentEnv } =
    resolveBackendConfig();

  return Response.json(
    {
      ok: true,
      vercelEnv,
      nodeEnv,
      deploymentEnv,
      backendUrl,
      source,
      checkedAt: new Date().toISOString(),
    },
    {
      headers: {
        'Cache-Control': 'no-store',
      },
    },
  );
}
