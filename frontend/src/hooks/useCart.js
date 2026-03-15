import { useState, useCallback, useEffect, useRef } from 'react';

const CART_KEY = 'cart_v1';

/**
 * Try to load saved cart from Telegram CloudStorage.
 * Falls back gracefully if not available (e.g., outside Telegram).
 */
function loadCartFromCloud(callback) {
  try {
    const cs = window.Telegram?.WebApp?.CloudStorage;
    if (cs) {
      cs.getItem(CART_KEY, (err, value) => {
        if (!err && value) {
          try {
            const parsed = JSON.parse(value);
            if (Array.isArray(parsed) && parsed.length > 0) {
              callback(parsed);
            }
          } catch (e) {
            // ignore corrupt data
          }
        }
      });
    }
  } catch (e) {
    // CloudStorage not available
  }
}

/**
 * Save cart to Telegram CloudStorage (fire-and-forget).
 */
function saveCartToCloud(items) {
  try {
    const cs = window.Telegram?.WebApp?.CloudStorage;
    if (cs) {
      cs.setItem(CART_KEY, JSON.stringify(items));
    }
  } catch (e) {
    // ignore
  }
}

export function useCart() {
  const [items, setItems] = useState([]);
  const loaded = useRef(false);

  // Load saved cart on mount
  useEffect(() => {
    if (!loaded.current) {
      loaded.current = true;
      loadCartFromCloud((savedItems) => {
        setItems(savedItems);
      });
    }
  }, []);

  // Save to cloud whenever items change (skip initial empty state)
  useEffect(() => {
    if (loaded.current) {
      saveCartToCloud(items);
    }
  }, [items]);

  const addItem = useCallback((product) => {
    setItems(prev => {
      const existing = prev.find(i => i.id === product.id);
      if (existing) {
        return prev.map(i =>
          i.id === product.id ? { ...i, quantity: i.quantity + 1 } : i
        );
      }
      return [...prev, { ...product, quantity: 1 }];
    });
  }, []);

  const removeItem = useCallback((productId) => {
    setItems(prev => prev.filter(i => i.id !== productId));
  }, []);

  const updateQuantity = useCallback((productId, quantity) => {
    if (quantity <= 0) {
      setItems(prev => prev.filter(i => i.id !== productId));
      return;
    }
    setItems(prev =>
      prev.map(i => (i.id === productId ? { ...i, quantity } : i))
    );
  }, []);

  const clearCart = useCallback(() => setItems([]), []);

  const totalCount = items.reduce((sum, i) => sum + i.quantity, 0);

  // Group totals by currency
  const totals = items.reduce((acc, item) => {
    const cur = item.currency || 'USD';
    acc[cur] = (acc[cur] || 0) + item.price * item.quantity;
    return acc;
  }, {});

  return { items, addItem, removeItem, updateQuantity, clearCart, totalCount, totals };
}
