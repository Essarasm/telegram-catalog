import { useState, useCallback, useEffect, useRef } from 'react';

const API = '/api/cart';

/**
 * Get the Telegram user ID. Returns 0 as fallback (for testing outside Telegram).
 */
function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

/**
 * Server-side cart hook.
 *
 * All cart state lives on the server (SQLite), keyed by Telegram user ID.
 * This eliminates every client-side persistence issue:
 * - No CloudStorage dependency
 * - No async timing / app-close race conditions
 * - No size limits
 * - No ID mismatch across deploys
 * - Works across devices
 */
export function useCart() {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const mountedOnce = useRef(false);
  const userId = useRef(getTelegramUserId());

  // Load cart from server on mount
  useEffect(() => {
    if (!mountedOnce.current) {
      mountedOnce.current = true;
      fetch(`${API}?user_id=${userId.current}`)
        .then(r => r.json())
        .then(data => {
          setItems(data.items || []);
          setLoading(false);
        })
        .catch(() => setLoading(false));
    }
  }, []);

  const addItem = useCallback((product) => {
    setItems(prev => {
      const existing = prev.find(i => i.id === product.id);
      const newQty = existing ? existing.quantity + 1 : 1;
      const next = existing
        ? prev.map(i => i.id === product.id ? { ...i, quantity: newQty } : i)
        : [...prev, {
            id: product.id,
            name: product.name_display || product.name,
            name_display: product.name_display || product.name,
            price: product.price,
            currency: product.currency,
            unit: product.unit,
            quantity: 1,
          }];

      // Fire-and-forget server sync
      fetch(`${API}/set`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: userId.current,
          product_id: product.id,
          quantity: newQty,
        }),
      }).catch(() => {});

      return next;
    });
  }, []);

  const removeItem = useCallback((productId) => {
    setItems(prev => prev.filter(i => i.id !== productId));
    fetch(`${API}/set`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        user_id: userId.current,
        product_id: productId,
        quantity: 0,
      }),
    }).catch(() => {});
  }, []);

  /**
   * Restore a previously-removed item with its original quantity.
   * Used by the cart's "Undo" toast after a × delete.
   * If the same product was re-added in the meantime, keeps the larger quantity.
   */
  const restoreItem = useCallback((item) => {
    setItems(prev => {
      const existing = prev.find(i => i.id === item.id);
      if (existing) {
        const merged = Math.max(existing.quantity, item.quantity);
        // Sync server with the merged quantity
        fetch(`${API}/set`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            user_id: userId.current,
            product_id: item.id,
            quantity: merged,
          }),
        }).catch(() => {});
        return prev.map(i => (i.id === item.id ? { ...i, quantity: merged } : i));
      }
      // Item was fully removed — add the snapshot back as-is
      fetch(`${API}/set`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: userId.current,
          product_id: item.id,
          quantity: item.quantity,
        }),
      }).catch(() => {});
      return [...prev, { ...item }];
    });
  }, []);

  const updateQuantity = useCallback((productId, quantity) => {
    if (quantity <= 0) {
      setItems(prev => prev.filter(i => i.id !== productId));
    } else {
      setItems(prev =>
        prev.map(i => (i.id === productId ? { ...i, quantity } : i))
      );
    }
    fetch(`${API}/set`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        user_id: userId.current,
        product_id: productId,
        quantity: Math.max(0, quantity),
      }),
    }).catch(() => {});
  }, []);

  const clearCart = useCallback(() => {
    setItems([]);
    fetch(`${API}/clear`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_id: userId.current }),
    }).catch(() => {});
  }, []);

  const reloadCart = useCallback(() => {
    return fetch(`${API}?user_id=${userId.current}`)
      .then(r => r.json())
      .then(data => {
        setItems(data.items || []);
        return data.items || [];
      })
      .catch(() => []);
  }, []);

  const totalCount = items.reduce((sum, i) => sum + i.quantity, 0);

  const totals = items.reduce((acc, item) => {
    const cur = item.currency || 'USD';
    acc[cur] = (acc[cur] || 0) + item.price * item.quantity;
    return acc;
  }, {});

  return { items, loading, addItem, removeItem, restoreItem, updateQuantity, clearCart, reloadCart, totalCount, totals };
}
