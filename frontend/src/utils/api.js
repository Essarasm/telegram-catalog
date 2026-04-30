const API_BASE = '/api';

export async function fetchCategories() {
  const res = await fetch(`${API_BASE}/categories`);
  return res.json();
}

export async function fetchProducers(categoryId) {
  const res = await fetch(`${API_BASE}/categories/${categoryId}/producers`);
  return res.json();
}

export async function fetchProducts({ categoryId, producerId, search, page = 1, limit = 30, telegramId }) {
  const params = new URLSearchParams();
  if (categoryId) params.set('category_id', categoryId);
  if (producerId) params.set('producer_id', producerId);
  if (search) params.set('search', search);
  if (telegramId) params.set('telegram_id', telegramId);
  params.set('page', page);
  params.set('limit', limit);
  const res = await fetch(`${API_BASE}/products?${params}`);
  return res.json();
}

// ── Search analytics ────────────────────────────────────────────

export async function logSearchClick({ searchLogId, telegramId, productId, action = 'click' }) {
  try {
    const params = new URLSearchParams({
      search_log_id: searchLogId || 0,
      telegram_id: telegramId || 0,
      product_id: productId,
      action,
    });
    fetch(`${API_BASE}/search/click?${params}`, { method: 'POST' });
  } catch (e) { /* silent — analytics should never break UX */ }
}

export async function logInterestClick({ telegramId, productId, searchQuery = '', matchScore = 0 }) {
  try {
    const params = new URLSearchParams({
      telegram_id: telegramId || 0,
      product_id: productId,
      search_query: searchQuery,
      match_score: matchScore,
    });
    fetch(`${API_BASE}/search/interest-click?${params}`, { method: 'POST' });
  } catch (e) { /* silent */ }
}

// ── Search suggestions & "Did you mean?" ────────────────────────

export async function fetchSearchSuggestions(query) {
  if (!query || query.length < 1) return { suggestions: [], total_matches: 0 };
  try {
    const res = await fetch(`${API_BASE}/search/suggestions?q=${encodeURIComponent(query)}&limit=6`);
    const data = await res.json();
    return {
      suggestions: Array.isArray(data.suggestions) ? data.suggestions : [],
      total_matches: data.total_matches || 0,
    };
  } catch (e) { return { suggestions: [], total_matches: 0 }; }
}

export async function fetchDidYouMean(query) {
  if (!query || query.length < 2) return [];
  try {
    const res = await fetch(`${API_BASE}/search/did-you-mean?q=${encodeURIComponent(query)}&limit=3`);
    const data = await res.json();
    return data.suggestions || [];
  } catch (e) { return []; }
}

export async function exportOrder(items, format = 'pdf', clientName = '', telegramId = 0) {
  const res = await fetch(`${API_BASE}/export`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      items: items.map(i => ({ product_id: i.id, quantity: i.quantity })),
      format,
      client_name: clientName,
      telegram_id: telegramId,
    }),
  });
  return res.blob();
}

export function formatPrice(priceUsd, priceUzs) {
  // Show USD price preferentially
  if (priceUsd && priceUsd > 0) {
    return `$${Number(priceUsd).toFixed(2)}`;
  }
  if (priceUzs && priceUzs > 0) {
    return `${Number(priceUzs).toLocaleString('uz-UZ')} so'm`;
  }
  return '—';
}

export function getPriceCurrency(priceUsd, priceUzs) {
  if (priceUsd && priceUsd > 0) return 'USD';
  if (priceUzs && priceUzs > 0) return 'UZS';
  return 'USD';
}

export function getPriceValue(priceUsd, priceUzs) {
  if (priceUsd && priceUsd > 0) return priceUsd;
  if (priceUzs && priceUzs > 0) return priceUzs;
  return 0;
}

// Format a single price with known currency (used in cart)
export function formatCartPrice(amount, currency) {
  if (!amount || amount <= 0) return '—';
  if (currency === 'UZS') {
    return `${Number(amount).toLocaleString('uz-UZ')} so'm`;
  }
  return `$${Number(amount).toFixed(2)}`;
}

export async function submitReport({ productId, telegramId, reportType, note }) {
  const res = await fetch(`${API_BASE}/reports`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      product_id: productId,
      telegram_id: telegramId,
      report_type: reportType,
      note: note || null,
    }),
  });
  return res.json();
}

export async function submitProductRequest({ telegramId, requestText }) {
  const res = await fetch(`${API_BASE}/product-requests`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      telegram_id: telegramId,
      request_text: requestText,
    }),
  });
  return res.json();
}

// ── Agent panel ────────────────────────────────────────────────

export async function fetchFxRateToday() {
  try {
    const res = await fetch(`${API_BASE}/finance/fx-rate/today`);
    return res.json();
  } catch (e) {
    return { ok: false, is_stale: true, today_events: [], yesterday: null };
  }
}

export async function searchAgentClients(telegramId, q) {
  const params = new URLSearchParams({ telegram_id: telegramId, q });
  const res = await fetch(`${API_BASE}/agent/search-clients?${params}`);
  return res.json();
}

export async function switchAgentClient(payload) {
  const res = await fetch(`${API_BASE}/agent/switch-client`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function fetchRecentAgentClients(telegramId) {
  try {
    const res = await fetch(`${API_BASE}/agent/recent-clients?telegram_id=${telegramId}`);
    return res.json();
  } catch (e) {
    return { ok: false, recent: [] };
  }
}

export async function fetchCabinetClientInfo(telegramId) {
  try {
    const res = await fetch(`${API_BASE}/cabinet/client-info?telegram_id=${telegramId}`);
    return res.json();
  } catch (e) {
    return { ok: false, client: null };
  }
}

export function getImageUrl(product) {
  if (product.image_path) {
    return `/images/${product.image_path}`;
  }
  return null;
}

// ── Cashbook (Session Z — Phase 1) ──────────────────────────────

export async function submitAgentCashHandover({ telegramId, clientId, uzsAmount, usdAmount, force = false }) {
  const res = await fetch(`${API_BASE}/payments/agent-cash-handover`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      telegram_id: telegramId,
      client_id: clientId,
      uzs_amount: uzsAmount || 0,
      usd_amount: usdAmount || 0,
      force,
    }),
  });
  const data = await res.json();
  return { status: res.status, ...data };
}

export async function fetchMyPendingPayments(telegramId) {
  try {
    const res = await fetch(`${API_BASE}/payments/my-pending?telegram_id=${telegramId}`);
    return res.json();
  } catch (e) {
    return { ok: false, items: [] };
  }
}

export async function fetchPendingForClient(telegramId, clientId) {
  try {
    const params = new URLSearchParams({ telegram_id: telegramId });
    if (clientId) params.set('client_id', clientId);
    const res = await fetch(`${API_BASE}/payments/pending-for-client?${params}`);
    return res.json();
  } catch (e) {
    return { ok: false, items: [] };
  }
}

export async function cancelIntakePayment({ telegramId, paymentId, reason }) {
  const res = await fetch(`${API_BASE}/payments/cancel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      telegram_id: telegramId,
      payment_id: paymentId,
      reason: reason || '',
    }),
  });
  const data = await res.json();
  return { status: res.status, ...data };
}
