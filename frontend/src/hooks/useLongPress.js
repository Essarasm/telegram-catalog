import { useRef, useCallback } from 'react';

/**
 * Long-press auto-repeat hook for ± quantity buttons.
 *
 * Quick tap fires action once (normal behavior).
 * Hold down → after 400ms starts repeating, accelerating over time.
 * Includes haptic feedback on each tick (Telegram WebApp).
 *
 * The callback can return `false` to signal "stop repeating" —
 * used by the − button to stop at quantity 1 (require a deliberate
 * single tap to remove the item entirely).
 *
 * Usage:
 *   const bind = useLongPress(() => {
 *     if (qty <= 1) return false; // stop repeating
 *     updateQty(id, qty - 1);
 *   });
 *   <button {...bind}>−</button>
 */
export function useLongPress(callback, { initialDelay = 400, minInterval = 60 } = {}) {
  const timerRef = useRef(null);
  const callbackRef = useRef(callback);
  callbackRef.current = callback;

  const haptic = () => {
    try { window.Telegram?.WebApp?.HapticFeedback?.impactOccurred('light'); } catch {}
  };

  const stop = useCallback(() => {
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
  }, []);

  const start = useCallback((e) => {
    e.preventDefault();
    stop();

    let tickCount = 0;

    timerRef.current = setTimeout(() => {
      const tick = () => {
        const result = callbackRef.current();
        // If callback returns false, stop repeating
        if (result === false) {
          stop();
          return;
        }
        haptic();
        tickCount++;

        const nextDelay = tickCount < 5 ? 200 : tickCount < 15 ? 100 : minInterval;
        timerRef.current = setTimeout(tick, nextDelay);
      };
      tick();
    }, initialDelay);
  }, [stop, initialDelay, minInterval]);

  return {
    onPointerDown: start,
    onPointerUp: stop,
    onPointerLeave: stop,
    onContextMenu: (e) => e.preventDefault(),
  };
}
