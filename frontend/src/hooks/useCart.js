import { useState, useCallback, useEffect, useRef } from 'react';

const CART_KEY = 'cart_v2'; // bumped to avoid loading old bloated data

/**
 * Compress a cart item for CloudStorage.
 * Short keys: i=id, n=name, p=price, c=currency, u=unit, q=quantity
 * ~65 chars per item → supports 60+ items within the 4096-char limit.
 */
function compress(item) {
  return {
    i: item.id,
    n: (item.name_display || item.name || '').slice(0, 18),
    p: item.price,
    c: item.currency === 'UZS' ? 'Z' : 'D',
    q: item.quantity,
  };
}

/**
 * Decompress a cart item from CloudStorage back to full field names.
 */
function decompress(raw) {
  // Handle both compressed (short keys) and legacy (full keys) formats
  if (raw.i !== undefined) {
    // Compressed format — expand 1-char currency back
    const currency = raw.c === 'Z' ? 'UZS' : (raw.c === 'D' ? 'USD' : raw.c);
    return {
      id: raw.i,
      name: raw.n,
      name_display: raw.n,
      price: raw.p,
      currency,
      unit: raw.u || '',
      quantity: raw.q,
    };
  }
  // Legacy full-key format — normalize it
  return {
    id: raw.id,
    name: raw.name_display || raw.name || '',
    name_display: raw.name_display || raw.name || '',
    price: raw.price,
    currency: raw.currency,
    unit: raw.unit,
    quantity: raw.quantity || 1,
  };
}

/**
 * Try to load saved cart from Telegram CloudStorage.
 */
function loadCartFromCloud(callback) {
  try {
    const cs = window.Telegram?.WebApp?.CloudStorage;
    if (!cs) return;

    cs.getItem(CART_KEY, (err, value) => {
      if (!err && value) {
        try {
          const parsed = JSON.parse(value);
          if (Array.isArray(parsed) && parsed.length > 0) {
            callback(parsed.map(decompress));
            return;
          }
        } catch (e) { /* ignore corrupt */ }
      }
      // Also try loading old key for migration
      cs.getItem('cart_v1', (err2, value2) => {
        if (!err2 && value2) {
          try {
            const parsed2 = JSON.parse(value2);
            if (Array.isArray(parsed2) && parsed2.length > 0) {
              callback(parsed2.map(decompress));
              // Migrate: save under new key and delete old
              cs.setItem(CART_KEY, JSON.stringify(parsed2.map(compress)));
              cs.removeItem('cart_v1');
            }
          } catch (e) { /* ignore */ }
        }
      });
    });
  } catch (e) {
    // CloudStorage not available
  }
}

/**
 * Save cart to Telegram CloudStorage using compressed format.
 */
function saveCartToCloud(items) {
  try {
    const cs = window.Telegram?.WebApp?.CloudStorage;
    if (!cs) return;

    const payload = JSON.stringify(items.map(compress));
    // Safety: check length before saving (4096-char limit)
    if (payload.length <= 4096) {
      cs.setItem(CART_KEY, payload);
    } else {
      console.warn(`Cart too large for CloudStorage: ${payload.length} chars`);
      // Save as much as possible: trim from the end
      let trimmed = [...items];
      while (trimmed.length > 0) {
        const p = JSON.stringify(trimmed.map(compress));
        if (p.length <= 4096) {
          cs.setItem(CART_KEY, p);
          break;
        }
        trimmed.pop();
      }
    }
  } catch (e) {
    // ignore
  }
}

export function useCart() {
  const [items, setItems] = useState([]);
  const cloudLoaded = useRef(false);
  const mountedOnce = useRef(false);

  // Load saved cart on mount
  useEffect(() => {
    if (!mountedOnce.current) {
      mountedOnce.current = true;
      loadCartFromCloud((savedItems) => {
        cloudLoaded.current = true;
        setItems(savedItems);
      });
      // Fallback: if CloudStorage is empty/missing, allow saves after 1s
      // (but DON'T save the empty array — only allow future saves)
      const timer = setTimeout(() => {
        if (!cloudLoaded.current) {
          cloudLoaded.current = true;
        }
      }, 1000);
      return () => clearTimeout(timer);
    }
  }, []);

  // Save to cloud whenever items change — but ONLY after cloud load finishes,
  // and ONLY if there are items (never overwrite with empty unless user cleared)
  const userHasInteracted = useRef(false);
  useEffect(() => {
    if (cloudLoaded.current) {
      if (items.length > 0) {
        saveCartToCloud(items);
      } else if (userHasInteracted.current) {
        // Only save empty array if user explicitly cleared the cart
        saveCartToCloud([]);
      }
    }
  }, [items]);

  const addItem = useCallback((product) => {
    userHasInteracted.current = true;
    setItems(prev => {
      const existing = prev.find(i => i.id === product.id);
      if (existing) {
        return prev.map(i =>
          i.id === product.id ? { ...i, quantity: i.quantity + 1 } : i
        );
      }
      return [...prev, {
        id: product.id,
        name: product.name_display || product.name,
        name_display: product.name_display || product.name,
        price: product.price,
        currency: product.currency,
        unit: product.unit,
        quantity: 1,
      }];
    });
  }, []);

  const removeItem = useCallback((productId) => {
    userHasInteracted.current = true;
    setItems(prev => prev.filter(i => i.id !== productId));
  }, []);

  const updateQuantity = useCallback((productId, quantity) => {
    userHasInteracted.current = true;
    if (quantity <= 0) {
      setItems(prev => prev.filter(i => i.id !== productId));
      return;
    }
    setItems(prev =>
      prev.map(i => (i.id === productId ? { ...i, quantity } : i))
    );
  }, []);

  const clearCart = useCallback(() => {
    userHasInteracted.current = true;
    setItems([]);
  }, []);

  const totalCount = items.reduce((sum, i) => sum + i.quantity, 0);

  const totals = items.reduce((acc, item) => {
    const cur = item.currency || 'USD';
    acc[cur] = (acc[cur] || 0) + item.price * item.quantity;
    return acc;
  }, {});

  return { items, addItem, removeItem, updateQuantity, clearCart, totalCount, totals };
}
