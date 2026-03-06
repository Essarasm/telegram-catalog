const API_BASE = '/api';

export async function fetchCategories() {
  const res = await fetch(`${API_BASE}/categories`);
  return res.json();
}

export async function fetchProducts({ categoryId, search, page = 1, limit = 20 }) {
  const params = new URLSearchParams();
  if (categoryId) params.set('category_id', categoryId);
  if (search) params.set('search', search);
  params.set('page', page);
  params.set('limit', limit);
  const res = await fetch(`${API_BASE}/products?${params}`);
  return res.json();
}

export async function exportOrder(items, format = 'pdf', clientName = '') {
  const res = await fetch(`${API_BASE}/export`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      items: items.map(i => ({ product_id: i.id, quantity: i.quantity })),
      format,
      client_name: clientName,
    }),
  });
  return res.blob();
}

export function formatPrice(price, currency) {
  if (currency === 'UZS') {
    return `${Number(price).toLocaleString('uz-UZ')} so'm`;
  }
  return `$${Number(price).toFixed(2)}`;
}

export function getImageUrl(product) {
  if (product.image_path) {
    return `/images/${product.image_path}`;
  }
  return null;
}
