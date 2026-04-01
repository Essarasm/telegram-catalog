/**
 * Telegram CloudStorage helpers.
 *
 * Persist registration data client-side so users survive server-side
 * DB wipes during Railway deploys.
 *
 * cloudSave() returns a Promise<boolean>.  It has a 2-second timeout
 * fallback for older Telegram clients where the callback never fires.
 */

export function cloudSave(key, data) {
  return new Promise((resolve) => {
    try {
      const cs = window.Telegram?.WebApp?.CloudStorage;
      if (!cs) return resolve(false);
      cs.setItem(key, JSON.stringify(data), (err) => {
        if (err) {
          console.warn('[cloudSave] failed:', err);
          resolve(false);
        } else {
          resolve(true);
        }
      });
      // Fallback timeout — resolve after 2s even if callback never fires
      setTimeout(() => resolve(false), 2000);
    } catch (e) {
      console.warn('[cloudSave] exception:', e);
      resolve(false);
    }
  });
}

export function cloudLoad(key) {
  return new Promise((resolve) => {
    try {
      const cs = window.Telegram?.WebApp?.CloudStorage;
      if (!cs) return resolve(null);
      cs.getItem(key, (err, val) => {
        if (err || !val) return resolve(null);
        try { resolve(JSON.parse(val)); } catch { resolve(null); }
      });
    } catch { resolve(null); }
  });
}
