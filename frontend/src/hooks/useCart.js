import { useState, useCallback, useEffect, useRef } from 'react';

const CART_KEY = 'cart_v1';

/**
 * Only store essential fields to stay within Telegram CloudStorage's
 * 4096-byte-per-key limit.  ~80 bytes per item → safe up to ~50 items.
 */
function slimItem(product, quantity = 1) {
  return {
    id: product.id,
    name: product.name_display || product.name,
    name_display: product.name_display || product.name,
    price: product.price,
    currency: product.currency,
    unit: product.unit,
    quantity,
  };
}

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
  const cloudLoaded = useRef(false);   // true once the async cloud read finishes
  const mountedOnce = useRef(false);

  // Load saved cart on mount
  useEffect(() => {
    if (!mountedOnce.current) {
      mountedOnce.current = true;
      loadCartFromCloud((savedItems) => {
        cloudLoaded.current = true;
        setItems(savedItems);
      });
      // If CloudStorage is unavailable or empty, mark as loaded after a short delay
      // so that subsequent user additions still get saved.
      setTimeout(() => { cloudLoaded.current = true; }, 500);
    }
  }, []);

  // Save to cloud whenever items change — but ONLY after cloud read is done,
  // so we never overwrite saved data with an empty initial array.
  useEffect(() => {
    if (cloudLoaded.current) {
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
      return [...prev, slimItem(product)];
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
