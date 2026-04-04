import { useRef, useCallback } from 'react';

/**
 * Long-press auto-repeat hook for ± quantity buttons.
 *
 * Quick tap fires action once (normal behavior).
 * Hold down → after 400ms starts repeating, accelerating over time.
 * Includes haptic feedback on each tick (Telegram WebApp).
 *
 * Usage:
 *   const bind = useLongPress(() => updateQty(id, qty + 1));
 *   <button {...bind}>+</button>
 */
export function useLongPress(callback, { initialDelay = 400, minInterval = 60 } = {}) {
  const timerRef = useRef(null);
  const intervalRef = useRef(null);
  const callbackRef = useRef(callback);
  callbackRef.current = callback;

  const haptic = () => {
    try { window.Telegram?.WebApp?.HapticFeedback?.impactOccurred('light'); } catch {}
  };

  const stop = useCallback(() => {
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
    if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null; }
  }, []);

  const start = useCallback((e) => {
    // Prevent text selection and context menu on long press
    e.preventDefault();
    stop();

    // First tick: the normal tap fires the callback
    // (onClick handles the first increment, so we just start the repeat timer)
    let tickCount = 0;

    timerRef.current = setTimeout(() => {
      // Start repeating
      const tick = () => {
        callbackRef.current();
        haptic();
        tickCount++;

        // Accelerate: slow → medium → fast
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
    onContextMenu: (e) => e.preventDefault(), // prevent long-press context menu on mobile
  };
}
