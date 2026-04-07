import { useState, useRef, useEffect } from 'react';
import { getImageUrl, formatPrice, submitReport } from '../utils/api';
import t from '../i18n/uz.json';

const MAX_ZOOM = 4;
const DOUBLE_TAP_ZOOM = 2.5;

const WHOLESALE_QTYS = [6, 12, 15, 25, 36, 50];

const REPORT_TYPES = [
  { key: 'wrong_photo', label: t.report_wrong_photo, icon: '📷' },
  { key: 'rotated_photo', label: t.report_rotated_photo, icon: '🔄' },
  { key: 'wrong_price', label: t.report_wrong_price, icon: '💰' },
  { key: 'wrong_name', label: t.report_wrong_name, icon: '📝' },
  { key: 'wrong_category', label: t.report_wrong_category, icon: '📂' },
  { key: 'other', label: t.report_other, icon: '❓' },
];

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

export default function ProductDetailPage({ product, producer, cart, approved, onBack }) {
  const [showQtyPicker, setShowQtyPicker] = useState(false);
  const [customQty, setCustomQty] = useState('');
  const [showReportSheet, setShowReportSheet] = useState(false);
  const [selectedReportType, setSelectedReportType] = useState(null);
  const [reportNote, setReportNote] = useState('');
  const [reportSending, setReportSending] = useState(false);
  const [reportSent, setReportSent] = useState(false);

  const imgUrl = getImageUrl(product);
  const displayName = product.name_display || product.name;
  const priceStr = approved ? formatPrice(product.price_usd, product.price_uzs) : null;
  const inCart = cart.items.find(i => i.id === product.id);

  // ── Pinch / double-tap zoom on the product image ──────────────────────────
  const imgWrapRef = useRef(null);
  const [zoom, setZoom] = useState({ scale: 1, tx: 0, ty: 0 });
  const pointersRef = useRef(new Map());     // pointerId -> {x, y}
  const gestureRef = useRef(null);            // active gesture snapshot
  const lastTapRef = useRef(0);

  // Reset zoom whenever the displayed product changes
  useEffect(() => {
    setZoom({ scale: 1, tx: 0, ty: 0 });
    pointersRef.current.clear();
    gestureRef.current = null;
  }, [product.id]);

  const clampZoom = (scale, tx, ty) => {
    const s = Math.max(1, Math.min(MAX_ZOOM, scale));
    if (s === 1) return { scale: 1, tx: 0, ty: 0 };
    const rect = imgWrapRef.current?.getBoundingClientRect();
    if (!rect) return { scale: s, tx, ty };
    const maxX = (rect.width * (s - 1)) / 2;
    const maxY = (rect.height * (s - 1)) / 2;
    return {
      scale: s,
      tx: Math.max(-maxX, Math.min(maxX, tx)),
      ty: Math.max(-maxY, Math.min(maxY, ty)),
    };
  };

  const handleImgPointerDown = (e) => {
    e.currentTarget.setPointerCapture(e.pointerId);
    pointersRef.current.set(e.pointerId, { x: e.clientX, y: e.clientY });

    if (pointersRef.current.size === 2) {
      const [a, b] = Array.from(pointersRef.current.values());
      const dist = Math.hypot(b.x - a.x, b.y - a.y);
      gestureRef.current = {
        type: 'pinch',
        startDist: dist,
        startMid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 },
        startScale: zoom.scale,
        startTx: zoom.tx,
        startTy: zoom.ty,
        moved: false,
      };
    } else if (pointersRef.current.size === 1) {
      gestureRef.current = {
        type: 'pan',
        startX: e.clientX,
        startY: e.clientY,
        startTx: zoom.tx,
        startTy: zoom.ty,
        moved: false,
      };
    }
  };

  const handleImgPointerMove = (e) => {
    if (!pointersRef.current.has(e.pointerId)) return;
    pointersRef.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    const g = gestureRef.current;
    if (!g) return;

    if (g.type === 'pinch' && pointersRef.current.size === 2) {
      const [a, b] = Array.from(pointersRef.current.values());
      const dist = Math.hypot(b.x - a.x, b.y - a.y);
      const mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
      const newScale = g.startScale * (dist / g.startDist);
      const newTx = g.startTx + (mid.x - g.startMid.x);
      const newTy = g.startTy + (mid.y - g.startMid.y);
      g.moved = true;
      setZoom(clampZoom(newScale, newTx, newTy));
    } else if (g.type === 'pan' && pointersRef.current.size === 1 && zoom.scale > 1) {
      const newTx = g.startTx + (e.clientX - g.startX);
      const newTy = g.startTy + (e.clientY - g.startY);
      const dx = e.clientX - g.startX;
      const dy = e.clientY - g.startY;
      if (Math.hypot(dx, dy) > 4) g.moved = true;
      setZoom(clampZoom(zoom.scale, newTx, newTy));
    }
  };

  const handleImgPointerUp = (e) => {
    pointersRef.current.delete(e.pointerId);
    const g = gestureRef.current;

    if (pointersRef.current.size === 0) {
      // Last finger lifted — check for double-tap (only if no real movement)
      const wasTap = g && !g.moved && g.type === 'pan';
      if (wasTap) {
        const now = Date.now();
        if (now - lastTapRef.current < 300) {
          // Double-tap → toggle zoom
          setZoom(zoom.scale > 1 ? { scale: 1, tx: 0, ty: 0 } : { scale: DOUBLE_TAP_ZOOM, tx: 0, ty: 0 });
          lastTapRef.current = 0;
        } else {
          lastTapRef.current = now;
        }
      } else {
        lastTapRef.current = 0;
      }
      gestureRef.current = null;
    } else if (pointersRef.current.size === 1) {
      // Going from 2 fingers → 1 finger: switch to pan mode using the remaining pointer
      const [remaining] = Array.from(pointersRef.current.values());
      gestureRef.current = {
        type: 'pan',
        startX: remaining.x,
        startY: remaining.y,
        startTx: zoom.tx,
        startTy: zoom.ty,
        moved: true, // already moved during pinch — don't trigger double-tap
      };
    }
  };
  // ───────────────────────────────────────────────────────────────────────────

  const getPriceValue = () => {
    if (product.price_usd && product.price_usd > 0) return product.price_usd;
    return product.price_uzs || 0;
  };
  const getCurrency = () => {
    if (product.price_usd && product.price_usd > 0) return 'USD';
    return 'UZS';
  };

  const openQtyPicker = () => {
    setCustomQty(String(inCart?.quantity || 1));
    setShowQtyPicker(true);
  };

  const handleReportSubmit = async () => {
    if (!selectedReportType) return;
    setReportSending(true);
    try {
      await submitReport({
        productId: product.id,
        telegramId: getTelegramUserId(),
        reportType: selectedReportType,
        note: reportNote.trim() || null,
      });
      setReportSent(true);
      setTimeout(() => {
        setShowReportSheet(false);
        setReportSent(false);
        setSelectedReportType(null);
        setReportNote('');
      }, 1500);
    } catch (e) {
      // silently fail — user sees no change
    }
    setReportSending(false);
  };

  return (
    <div className="space-y-4">
      {/* Large product image — pinch / double-tap to zoom, flag button overlays top-right */}
      <div
        ref={imgWrapRef}
        className="relative w-full aspect-[3/4] bg-tg-secondary rounded-2xl overflow-hidden flex items-center justify-center"
      >
        {imgUrl ? (
          <img
            src={imgUrl}
            alt={displayName}
            draggable={false}
            className="w-full h-full object-contain select-none"
            style={{
              transform: `translate(${zoom.tx}px, ${zoom.ty}px) scale(${zoom.scale})`,
              transformOrigin: 'center center',
              transition: pointersRef.current.size === 0 ? 'transform 0.2s ease-out' : 'none',
              touchAction: 'none',
              willChange: 'transform',
            }}
            onPointerDown={handleImgPointerDown}
            onPointerMove={handleImgPointerMove}
            onPointerUp={handleImgPointerUp}
            onPointerCancel={handleImgPointerUp}
          />
        ) : (
          <span className="text-6xl opacity-20">📷</span>
        )}

        {/* Flag / report issue button — top-right corner (sibling overlay so it stays clickable) */}
        <button
          onClick={() => setShowReportSheet(true)}
          className="absolute top-2 right-2 z-10 w-10 h-10 rounded-full bg-amber-500/70 backdrop-blur-sm flex items-center justify-center active:bg-amber-600/80 transition-colors"
          title={t.report_issue}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z" />
            <line x1="4" y1="22" x2="4" y2="15" />
          </svg>
        </button>
      </div>

      {/* Producer badge */}
      {producer?.name && (
        <div className="inline-block bg-tg-button/10 text-tg-link text-xs font-semibold px-3 py-1 rounded-full">
          {producer.name}
        </div>
      )}

      {/* Full product name */}
      <h2 className="text-lg font-semibold leading-snug">
        {displayName}
      </h2>

      {/* Details row */}
      <div className="flex flex-wrap items-center gap-3 text-sm text-tg-hint">
        {product.unit && <span>{t.unit || 'Birlik'}: {product.unit}</span>}
        {product.weight ? <span>{product.weight} kg</span> : null}
        {product.stock_status === 'in_stock' && (
          <span className="inline-flex items-center gap-1 text-xs font-semibold px-2 py-0.5 rounded-full bg-green-500/15 text-green-600">
            <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
            {t.stock_in_stock}
          </span>
        )}
        {product.stock_status === 'low_stock' && (
          <span className="inline-flex items-center gap-1 text-xs font-semibold px-2 py-0.5 rounded-full bg-amber-500/15 text-amber-600">
            <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
            {t.stock_low_stock}
          </span>
        )}
        {product.stock_status === 'out_of_stock' && (
          <span className="inline-flex items-center gap-1 text-xs font-semibold px-2 py-0.5 rounded-full bg-red-500/15 text-red-500">
            <span className="w-1.5 h-1.5 rounded-full bg-red-500" />
            {t.stock_out_of_stock}
          </span>
        )}
      </div>

      {/* Price or contact message */}
      {approved ? (
        <div className="text-2xl font-bold text-tg-link">
          {priceStr}
        </div>
      ) : (
        <div className="bg-tg-secondary rounded-xl p-4 text-center">
          <div className="text-sm text-tg-hint">
            Narxlarni ko'rish uchun ro'yxatdan o'ting
          </div>
          <a
            href="https://t.me/axmatov0902"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block mt-2 text-tg-link text-sm font-medium"
          >
            Telegram orqali bog'lanish →
          </a>
        </div>
      )}

      {/* Add to cart / quantity controls — only for approved */}
      {approved && (
        <div className="pt-2">
          {inCart ? (
            <div className="flex items-center justify-center gap-4 bg-tg-secondary rounded-xl py-3">
              <button
                onClick={() => cart.updateQuantity(product.id, inCart.quantity - 1)}
                className="bg-tg-button text-tg-button-text font-bold text-xl w-10 h-10 rounded-full flex items-center justify-center"
              >
                −
              </button>
              <button
                onClick={openQtyPicker}
                className="text-xl font-semibold min-w-[40px] text-center px-3 py-1 rounded-lg bg-tg-button/15 active:bg-tg-button/30 transition-colors"
              >
                {inCart.quantity}
              </button>
              <button
                onClick={() => cart.updateQuantity(product.id, inCart.quantity + 1)}
                className="bg-tg-button text-tg-button-text font-bold text-xl w-10 h-10 rounded-full flex items-center justify-center"
              >
                +
              </button>
            </div>
          ) : (
            <button
              onClick={() => cart.addItem({
                ...product,
                price: getPriceValue(),
                currency: getCurrency(),
              })}
              className="w-full bg-tg-button text-tg-button-text font-semibold rounded-xl py-3 text-base active:scale-[0.98] transition-transform"
            >
              + {t.add_to_cart}
            </button>
          )}
        </div>
      )}

      {/* Bottom-sheet quantity picker */}
      {showQtyPicker && (
        <>
          <div
            className="fixed inset-0 bg-black/40 z-[100]"
            onClick={() => setShowQtyPicker(false)}
          />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl"
            style={{ maxHeight: '60vh' }}
          >
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
            <div className="text-center mb-4">
              <div className="text-sm font-semibold truncate">{displayName}</div>
              <div className="text-xs text-tg-hint mt-1">Miqdorni tanlang</div>
            </div>
            <div className="grid grid-cols-3 gap-2 mb-4">
              {WHOLESALE_QTYS.map(q => (
                <button
                  key={q}
                  onClick={() => { cart.updateQuantity(product.id, q); setShowQtyPicker(false); }}
                  className={`py-3 rounded-xl text-base font-bold transition-colors ${
                    inCart && inCart.quantity === q
                      ? 'bg-tg-button text-tg-button-text'
                      : 'bg-tg-secondary text-tg-text active:bg-tg-button active:text-tg-button-text'
                  }`}
                >
                  {q}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-2 mb-4">
              <input
                type="number"
                inputMode="numeric"
                placeholder="Boshqa son..."
                value={customQty}
                onChange={(e) => setCustomQty(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    const v = parseInt(customQty, 10);
                    if (v > 0) { cart.updateQuantity(product.id, v); setShowQtyPicker(false); }
                  }
                }}
                className="flex-1 rounded-xl px-4 py-3 text-base font-semibold text-center outline-none border border-tg-hint/30 focus:border-tg-link"
                style={{ color: 'var(--tg-theme-text-color)', backgroundColor: 'var(--tg-theme-secondary-bg-color)' }}
              />
              <button
                onClick={() => {
                  const v = parseInt(customQty, 10);
                  if (v > 0) { cart.updateQuantity(product.id, v); setShowQtyPicker(false); }
                }}
                className="bg-tg-button text-tg-button-text rounded-xl px-5 py-3 font-bold text-base"
              >
                ✓
              </button>
            </div>
            <button
              onClick={() => { cart.removeItem(product.id); setShowQtyPicker(false); }}
              className="w-full text-red-400 text-sm font-medium py-2"
            >
              O'chirish
            </button>
          </div>
        </>
      )}

      {/* Bottom-sheet: Report issue */}
      {showReportSheet && (
        <>
          <div
            className="fixed inset-0 bg-black/40 z-[100]"
            onClick={() => { if (!reportSending) { setShowReportSheet(false); setSelectedReportType(null); setReportNote(''); } }}
          />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl"
            style={{ maxHeight: '70vh' }}
          >
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />

            {reportSent ? (
              /* Success state */
              <div className="text-center py-6">
                <div className="text-3xl mb-3">✅</div>
                <div className="text-base font-semibold">{t.report_sent}</div>
                <div className="text-sm text-tg-hint mt-1">{t.report_thanks}</div>
              </div>
            ) : (
              /* Report form */
              <>
                <div className="text-center mb-4">
                  <div className="text-sm font-semibold">{t.report_issue}</div>
                  <div className="text-xs text-tg-hint mt-1 truncate">{displayName}</div>
                </div>

                {/* Report type buttons */}
                <div className="space-y-2 mb-4">
                  {REPORT_TYPES.map(({ key, label, icon }) => (
                    <button
                      key={key}
                      onClick={() => setSelectedReportType(key)}
                      className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl text-sm font-medium transition-colors ${
                        selectedReportType === key
                          ? 'bg-tg-button text-tg-button-text'
                          : 'bg-tg-secondary text-tg-text active:bg-tg-button/20'
                      }`}
                    >
                      <span className="text-base">{icon}</span>
                      {label}
                    </button>
                  ))}
                </div>

                {/* Optional note */}
                <textarea
                  placeholder={t.report_note_placeholder}
                  value={reportNote}
                  onChange={(e) => setReportNote(e.target.value)}
                  rows={2}
                  className="w-full rounded-xl px-4 py-3 text-sm outline-none border border-tg-hint/30 focus:border-tg-link resize-none mb-4"
                  style={{ color: 'var(--tg-theme-text-color)', backgroundColor: 'var(--tg-theme-secondary-bg-color)' }}
                />

                {/* Submit button */}
                <button
                  onClick={handleReportSubmit}
                  disabled={!selectedReportType || reportSending}
                  className={`w-full rounded-xl py-3 text-base font-semibold transition-all ${
                    selectedReportType && !reportSending
                      ? 'bg-tg-button text-tg-button-text active:scale-[0.98]'
                      : 'bg-tg-hint/20 text-tg-hint'
                  }`}
                >
                  {reportSending ? t.loading : t.report_submit}
                </button>
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
}
