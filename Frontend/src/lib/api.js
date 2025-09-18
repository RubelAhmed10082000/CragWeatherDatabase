const BASE = import.meta.env?.VITE_API_BASE_URL || '';

function getJSON(path, opts = {}) {
  const meta = document.querySelector('meta[name="api-base"]');
  const apiBase = meta ? (meta.content || '').trim().split(/\s+/)[0] : '';

  const url = path.startsWith('/api/')
    ? path
    : `${apiBase.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;

  return fetch(url, { credentials: 'same-origin', ...opts })
    .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`); return r.json(); });
}

