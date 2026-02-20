const FALLBACK_BACKEND_URL = 'http://localhost:8000';

function normalizeUrl(value) {
  return String(value || '')
    .trim()
    .replace(/\/+$/, '');
}

export function resolveBackendConfig() {
  const vercelEnv = String(process.env.VERCEL_ENV || '').trim();
  const nodeEnv = String(process.env.NODE_ENV || 'development').trim();
  const deploymentEnv = vercelEnv || `local-${nodeEnv}`;

  const productionUrl = normalizeUrl(process.env.REVIEW_BACKEND_URL_PRODUCTION);
  const previewUrl = normalizeUrl(process.env.REVIEW_BACKEND_URL_PREVIEW);
  const defaultUrl = normalizeUrl(process.env.REVIEW_BACKEND_URL);
  const publicUrl = normalizeUrl(process.env.NEXT_PUBLIC_BACKEND_URL);

  if (vercelEnv === 'production' && productionUrl) {
    return {
      backendUrl: productionUrl,
      source: 'REVIEW_BACKEND_URL_PRODUCTION',
      vercelEnv,
      nodeEnv,
      deploymentEnv,
    };
  }

  if (vercelEnv === 'preview' && previewUrl) {
    return {
      backendUrl: previewUrl,
      source: 'REVIEW_BACKEND_URL_PREVIEW',
      vercelEnv,
      nodeEnv,
      deploymentEnv,
    };
  }

  if (defaultUrl) {
    return {
      backendUrl: defaultUrl,
      source: 'REVIEW_BACKEND_URL',
      vercelEnv,
      nodeEnv,
      deploymentEnv,
    };
  }

  if (publicUrl) {
    return {
      backendUrl: publicUrl,
      source: 'NEXT_PUBLIC_BACKEND_URL',
      vercelEnv,
      nodeEnv,
      deploymentEnv,
    };
  }

  return {
    backendUrl: FALLBACK_BACKEND_URL,
    source: 'fallback(localhost)',
    vercelEnv,
    nodeEnv,
    deploymentEnv,
  };
}
