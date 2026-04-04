import { useRef, useCallback } from 'react';

/**
 * Long-press auto-repeat hook for ± quantity buttons.
 *
 * Quick tap  → fires `onTap` once (normal single-tap behavior).
 * Hold down  → after 400ms starts repeating `callback`, accelerating over time.
 * Lift after hold → onClick is suppressed (no accidental single-tap action).
 *
 * The repeat callback can return `false` to signal "stop repeating" —
 * used by the − button to stop at quantity 1 (require a deliberate
 * single tap to remove the item entirely).
 *
 * Usage:
 *   const bind = useLongPress(
 *     () => { if (qty <= 1) return false; updateQty(id, qty - 1); },
 *     { onTap: () => updateQty(id, qty - 1) }
 *   );
 *   <button {...bind}>−</button>
 */
export function useLongPress(callback, { onTap, initialDelay = 400, minInterval = 60 } = {}) {
  const timerRef = useRef(null);
  const callbackRef = useRef(callback);
  callbackRef.current = callback;
  const onTapRef = useRef(onTap);
  onTapRef.current = onTap;
  const didLongPress = useRef(false);

  const haptic = () => {
    try { window.Telegram?.WebApp?.HapticFeedback?.impactOccurred('light'); } catch {}
  };

  const stop = useCallback(() => {
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
  }, []);

  const start = useCallback((e) => {
    e.preventDefault();
    stop();
    didLongPress.current = false;

    let tickCount = 0;

    timerRef.current = setTimeout(() => {
      didLongPress.current = true;
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

  const handleClick = useCallback((e) => {
    // After a long press, suppress the click event entirely
    if (didLongPress.current) {
      e.preventDefault();
      e.stopPropagation();
      didLongPress.current = false;
      return;
    }
    // Normal single tap — fire onTap if provided
    if (onTapRef.current) {
      e.stopPropagation();
      onTapRef.current(e);
    }
  }, []);

  return {
    onPointerDown: start,
    onPointerUp: stop,
    onPointerLeave: stop,
    onClick: handleClick,
    onContextMenu: (e) => e.preventDefault(),
  };
}
