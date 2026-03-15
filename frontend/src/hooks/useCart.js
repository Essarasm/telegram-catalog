import { useState, useCallback, useEffect, useRef } from 'react';

const CART_KEY = 'cart_v3';

// ---------------------------------------------------------------------------
// CloudStorage helpers — store ONLY { id, quantity } per item (~20 bytes each)
// Product details (name, price, unit) are fetched from the server on load.
// This makes the payload tiny (~2000 chars for 100 items) and eliminates
// every size/encoding-related persistence bug.
// ---------------------------------------------------------------------------

function cloudGet(key) {
  return new Promise((resolve) => {
    try {
      const cs = window.Telegram?.WebApp?.CloudStorage;
      if (!cs) return resolve(null);
      cs.getItem(key, (err, val) => resolve(err ? null : val || null));
    } catch { resolve(null); }
  });
}

function cloudSet(key, value) {
  try {
    const cs = window.Telegram?.WebApp?.CloudStorage;
    if (cs) cs.setItem(key, value);
  } catch { /* best-effort */ }
}

function cloudRemove(key) {
  try {
    const cs = window.Telegram?.WebApp?.CloudStorage;
    if (cs) cs.removeItem(key);
  } catch { /* ignore */ }
}

/**
 * Persist the cart to CloudStorage.  Only IDs + quantities are stored.
 * Called synchronously inside every state-setter so the write starts
 * as early as possible (before the next React render, not after it).
 */
function persistCart(items) {
  const minimal = items.map(it => [it.id, it.quantity]); // [[id,qty], ...]
  cloudSet(CART_KEY, JSON.stringify(minimal));
}

/**
 * Load cart skeleton from CloudStorage, then hydrate from the server.
 */
async function loadCart() {
  // Try v3 first (array of [id, qty] pairs)
  let raw = await cloudGet(CART_KEY);
  let pairs = null;

  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) {
        // v3 format: [[id,qty], [id,qty], ...]
        if (Array.isArray(parsed[0])) {
          pairs = parsed.map(p => ({ id: p[0], quantity: p[1] }));
        }
        // Could also be v2 compressed objects — migrate
        else if (typeof parsed[0] === 'object') {
          pairs = parsed.map(p => ({
            id: p.i ?? p.id,
            quantity: p.q ?? p.quantity ?? 1,
          }));
        }
      }
    } catch { /* corrupt */ }
  }

  // Fallback: try old keys for migration
  if (!pairs) {
    for (const oldKey of ['cart_v2', 'cart_v1']) {
      const oldRaw = await cloudGet(oldKey);
      if (oldRaw) {
        try {
          const oldParsed = JSON.parse(oldRaw);
          if (Array.isArray(oldParsed) && oldParsed.length > 0) {
            pairs = oldParsed.map(p => ({
              id: p.i ?? p.id,
              quantity: p.q ?? p.quantity ?? 1,
            }));
            // Migrate to v3 and clean up
            const minimal = pairs.map(p => [p.id, p.quantity]);
            cloudSet(CART_KEY, JSON.stringify(minimal));
            cloudRemove(oldKey);
            break;
          }
        } catch { /* ignore */ }
      }
    }
  }

  if (!pairs || pairs.length === 0) return [];

  // Hydrate from server — fetch full product details
  try {
    const ids = pairs.map(p => p.id).join(',');
    const res = await fetch(`/api/products/by-ids?ids=${ids}`);
    const data = await res.json();
    const productMap = {};
    for (const p of data.items || []) {
      productMap[p.id] = p;
    }

    return pairs
      .filter(p => productMap[p.id]) // skip products that no longer exist
      .map(p => {
        const prod = productMap[p.id];
        const hasUsd = prod.price_usd && prod.price_usd > 0;
        return {
          id: prod.id,
          name: prod.name_display || prod.name,
          name_display: prod.name_display || prod.name,
          price: hasUsd ? prod.price_usd : (prod.price_uzs || 0),
          currency: hasUsd ? 'USD' : 'UZS',
          unit: prod.unit || '',
          quantity: p.quantity,
        };
      });
  } catch (err) {
    // API failed — return skeleton items with IDs so we don't lose the cart
    // They won't have names/prices, but the IDs are preserved for next load
    return pairs.map(p => ({
      id: p.id,
      name: '...',
      name_display: '...',
      price: 0,
      currency: 'USD',
      unit: '',
      quantity: p.quantity,
    }));
  }
}


export function useCart() {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const mountedOnce = useRef(false);

  // Load cart on mount
  useEffect(() => {
    if (!mountedOnce.current) {
      mountedOnce.current = true;
      loadCart().then(loaded => {
        setItems(loaded);
        setLoading(false);
      });
    }
  }, []);

  // Save on page hide / visibility change (last chance before WebView dies)
  useEffect(() => {
    const saveOnHide = () => {
      if (items.length > 0) persistCart(items);
    };
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'hidden') saveOnHide();
    });
    window.addEventListener('pagehide', saveOnHide);
    window.addEventListener('beforeunload', saveOnHide);
    return () => {
      document.removeEventListener('visibilitychange', saveOnHide);
      window.removeEventListener('pagehide', saveOnHide);
      window.removeEventListener('beforeunload', saveOnHide);
    };
  }, [items]);

  const addItem = useCallback((product) => {
    setItems(prev => {
      const existing = prev.find(i => i.id === product.id);
      let next;
      if (existing) {
        next = prev.map(i =>
          i.id === product.id ? { ...i, quantity: i.quantity + 1 } : i
        );
      } else {
        next = [...prev, {
          id: product.id,
          name: product.name_display || product.name,
          name_display: product.name_display || product.name,
          price: product.price,
          currency: product.currency,
          unit: product.unit,
          quantity: 1,
        }];
      }
      persistCart(next); // save IMMEDIATELY, not in an effect
      return next;
    });
  }, []);

  const removeItem = useCallback((productId) => {
    setItems(prev => {
      const next = prev.filter(i => i.id !== productId);
      persistCart(next);
      return next;
    });
  }, []);

  const updateQuantity = useCallback((productId, quantity) => {
    setItems(prev => {
      let next;
      if (quantity <= 0) {
        next = prev.filter(i => i.id !== productId);
      } else {
        next = prev.map(i => (i.id === productId ? { ...i, quantity } : i));
      }
      persistCart(next);
      return next;
    });
  }, []);

  const clearCart = useCallback(() => {
    setItems([]);
    persistCart([]);
  }, []);

  const totalCount = items.reduce((sum, i) => sum + i.quantity, 0);

  const totals = items.reduce((acc, item) => {
    const cur = item.currency || 'USD';
    acc[cur] = (acc[cur] || 0) + item.price * item.quantity;
    return acc;
  }, {});

  return { items, loading, addItem, removeItem, updateQuantity, clearCart, totalCount, totals };
}
