const BASE = import.meta.env?.VITE_API_BASE_URL || '';

export async function getJSON(path, init) {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return res.json();
}

