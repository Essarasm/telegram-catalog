import { useState, useRef, useEffect } from 'react';
import { formatCartPrice } from '../utils/api';
import { useLongPress } from '../hooks/useLongPress';
import t from '../i18n/uz.json';

const API_BASE = '/api';

/* ───────────────────────────────────────────
   Order Preview — HTML table mirroring the PDF
   ─────────────────────────────────────────── */
function OrderPreview({ items, onConfirm, onBack, exporting }) {
  const tgUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
  const clientName = tgUser
    ? `${tgUser.first_name || ''} ${tgUser.last_name || ''}`.trim()
    : '';

  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const dateStr = `${pad(now.getDate())}.${pad(now.getMonth() + 1)}.${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}`;

  const usdItems = items.filter(i => (i.currency || 'USD') === 'USD');
  const uzsItems = items.filter(i => (i.currency || 'USD') === 'UZS');

  const fmt = (val, cur) => {
    if (cur === 'UZS') return `${Number(val).toLocaleString('uz-UZ')} so'm`;
    return `$${Number(val).toFixed(2)}`;
  };

  const renderTable = (list, currency) => {
    let grandTotal = 0;
    return (
      <div className="mb-4">
        <div className="text-sm font-semibold mb-1.5" style={{ color: 'var(--tg-theme-text-color)' }}>
          Mahsulotlar ({currency})
        </div>
        <div className="overflow-x-auto rounded-lg border" style={{ borderColor: 'var(--tg-theme-hint-color, #999)', borderWidth: '0.5px' }}>
          <table className="w-full text-xs" style={{ borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ backgroundColor: '#2563EB', color: '#fff' }}>
                <th className="px-2 py-1.5 text-center font-semibold">#</th>
                <th className="px-2 py-1.5 text-left font-semibold">Mahsulot nomi</th>
                <th className="px-2 py-1.5 text-center font-semibold">Birlik</th>
                <th className="px-2 py-1.5 text-right font-semibold">Miqdor</th>
                <th className="px-2 py-1.5 text-right font-semibold">Narx</th>
                <th className="px-2 py-1.5 text-right font-semibold">Jami</th>
              </tr>
            </thead>
            <tbody>
              {list.map((item, idx) => {
                const qty = item.quantity || 1;
                const price = item.price || 0;
                const total = qty * price;
                grandTotal += total;
                return (
                  <tr key={item.id} style={{ backgroundColor: idx % 2 === 0 ? 'var(--tg-theme-bg-color)' : 'var(--tg-theme-secondary-bg-color)' }}>
                    <td className="px-2 py-1.5 text-center" style={{ borderBottom: '0.5px solid var(--tg-theme-hint-color, #ddd)' }}>{idx + 1}</td>
                    <td className="px-2 py-1.5 text-left" style={{ borderBottom: '0.5px solid var(--tg-theme-hint-color, #ddd)' }}>{item.name_display || item.name}</td>
                    <td className="px-2 py-1.5 text-center" style={{ borderBottom: '0.5px solid var(--tg-theme-hint-color, #ddd)' }}>{item.unit || 'шт'}</td>
                    <td className="px-2 py-1.5 text-right" style={{ borderBottom: '0.5px solid var(--tg-theme-hint-color, #ddd)' }}>{qty}</td>
                    <td className="px-2 py-1.5 text-right" style={{ borderBottom: '0.5px solid var(--tg-theme-hint-color, #ddd)' }}>{fmt(price, currency)}</td>
                    <td className="px-2 py-1.5 text-right font-medium" style={{ borderBottom: '0.5px solid var(--tg-theme-hint-color, #ddd)' }}>{fmt(total, currency)}</td>
                  </tr>
                );
              })}
              <tr style={{ backgroundColor: 'var(--tg-theme-secondary-bg-color)' }}>
                <td colSpan="5" className="px-2 py-2 text-right font-bold">JAMI:</td>
                <td className="px-2 py-2 text-right font-bold">{fmt(grandTotal, currency)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div>
      {/* Header */}
      <div className="text-center mb-3">
        <div className="text-base font-bold">BUYURTMA / ЗАКАЗ</div>
        <div className="text-xs text-tg-hint mt-1">Sana: {dateStr}</div>
        {clientName && <div className="text-xs text-tg-hint">Mijoz: {clientName}</div>}
      </div>

      {/* Tables by currency */}
      {usdItems.length > 0 && renderTable(usdItems, 'USD')}
      {uzsItems.length > 0 && renderTable(uzsItems, 'UZS')}

      {/* Action buttons */}
      <div className="space-y-2 mt-4">
        <button
          onClick={onConfirm}
          disabled={exporting}
          className="w-full bg-tg-button text-tg-button-text rounded-xl py-3 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
        >
          {exporting ? t.loading : '✅ Tasdiqlash va yuborish'}
        </button>
        <button
          onClick={onBack}
          disabled={exporting}
          className="w-full text-center text-sm py-2 disabled:opacity-50"
          style={{ color: 'var(--tg-theme-link-color)' }}
        >
          ← {t.back}
        </button>
      </div>
    </div>
  );
}

/* Cart item quantity stepper with long-press auto-repeat */
function CartQtyControls({ item, cart }) {
  const decBind = useLongPress(
    () => {
      const qty = cart.items.find(i => i.id === item.id)?.quantity || 1;
      if (qty <= 1) return false; // stop at 1 — require single tap to remove
      cart.updateQuantity(item.id, qty - 1);
    },
    { onTap: () => cart.updateQuantity(item.id, item.quantity - 1) }
  );
  const incBind = useLongPress(
    () => cart.updateQuantity(item.id, (cart.items.find(i => i.id === item.id)?.quantity || 0) + 1),
    { onTap: () => cart.updateQuantity(item.id, item.quantity + 1) }
  );

  return (
    <div className="flex items-center gap-1.5">
      <button
        {...decBind}
        className="w-9 h-9 rounded-full bg-tg-button text-tg-button-text flex items-center justify-center font-bold text-lg select-none no-callout"
      >
        −
      </button>
      <span className="text-sm font-bold min-w-[40px] text-center py-1.5 px-2 select-none no-callout">
        {item.quantity}
      </span>
      <button
        {...incBind}
        className="w-9 h-9 rounded-full bg-tg-button text-tg-button-text flex items-center justify-center font-bold text-lg select-none no-callout"
      >
        +
      </button>
    </div>
  );
}

const UNDO_WINDOW_MS = 4000;

export default function CartPage({ cart }) {
  const [exporting, setExporting] = useState(false);
  const [exported, setExported] = useState(false);
  const [previewFormat, setPreviewFormat] = useState(null); // 'pdf' | 'xlsx' | null

  // Snapshot of the most recently removed item, kept for ~4s so the user can undo.
  const [removedItem, setRemovedItem] = useState(null);
  const undoTimerRef = useRef(null);

  // Clear any pending undo timer on unmount
  useEffect(() => () => {
    if (undoTimerRef.current) clearTimeout(undoTimerRef.current);
  }, []);

  const handleRemoveItem = (item) => {
    // Snapshot the item before removing so undo can restore it with full quantity
    const snapshot = { ...item };
    cart.removeItem(item.id);
    setRemovedItem(snapshot);

    // Reset/refresh the undo timer
    if (undoTimerRef.current) clearTimeout(undoTimerRef.current);
    undoTimerRef.current = setTimeout(() => {
      setRemovedItem(null);
      undoTimerRef.current = null;
    }, UNDO_WINDOW_MS);

    // Light haptic feedback if available
    window.Telegram?.WebApp?.HapticFeedback?.impactOccurred?.('light');
  };

  const handleUndoRemove = () => {
    if (!removedItem) return;
    cart.restoreItem(removedItem);
    setRemovedItem(null);
    if (undoTimerRef.current) {
      clearTimeout(undoTimerRef.current);
      undoTimerRef.current = null;
    }
    window.Telegram?.WebApp?.HapticFeedback?.impactOccurred?.('light');
  };

  const handleExport = async (format) => {
    setExporting(true);
    try {
      const tgUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
      const clientName = tgUser
        ? `${tgUser.first_name || ''} ${tgUser.last_name || ''}`.trim()
        : '';
      const telegramId = tgUser?.id || 0;

      const itemsPayload = cart.items.map(i => ({ product_id: i.id, quantity: i.quantity }));

      const res = await fetch(`${API_BASE}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          items: itemsPayload,
          format,
          client_name: clientName,
          telegram_id: telegramId,
        }),
      });

      // Check if the backend sent the file via Telegram DM (JSON response)
      const contentType = res.headers.get('Content-Type') || '';
      if (contentType.includes('application/json')) {
        const json = await res.json();
        if (json.ok && json.sent_to_telegram) {
          // File sent to user's Telegram DM — success!
          setExported('telegram');
          setPreviewFormat(null);
          cart.clearCart();
          setExporting(false);
          return;
        }
      }

      // Fallback: bot DM failed, use browser download
      const tgApp = window.Telegram?.WebApp;
      const downloadToken = res.headers.get('X-Download-Token');

      if (tgApp?.openLink && downloadToken) {
        const origin = window.location.origin;
        const downloadUrl = `${origin}${API_BASE}/export/download/${downloadToken}`;
        tgApp.openLink(downloadUrl);
      } else {
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `buyurtma.${format === 'xlsx' ? 'xlsx' : 'pdf'}`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(() => URL.revokeObjectURL(url), 5000);
      }
      setExported('download');
      setPreviewFormat(null);
      cart.clearCart();
    } catch (err) {
      console.error('Export failed:', err);
    }
    setExporting(false);
  };

  if (cart.loading) {
    return <div className="text-center py-16 text-tg-hint">{t.loading}</div>;
  }

  if (cart.items.length === 0) {
    return (
      <div className="text-center py-16">
        <div className="text-5xl mb-4">🛒</div>
        <div className="text-lg font-medium">{t.cart_empty}</div>
        <div className="text-sm text-tg-hint mt-1">{t.cart_empty_desc}</div>
      </div>
    );
  }

  // ─── Preview mode: show order table before sending ───
  if (previewFormat) {
    return (
      <div>
        <OrderPreview
          items={cart.items}
          exporting={exporting}
          onConfirm={() => handleExport(previewFormat)}
          onBack={() => setPreviewFormat(null)}
        />
      </div>
    );
  }

  return (
    <div>
      {/* Cart items */}
      <div className="space-y-2 mb-6">
        {cart.items.map(item => (
          <div key={item.id} className="bg-tg-secondary rounded-xl p-3">
            <div className="flex items-center gap-2">
              <div className="flex-1 min-w-0">
                <div className="text-sm font-medium leading-tight truncate">{item.name_display || item.name}</div>
                <div className="text-xs text-tg-hint mt-0.5">
                  {formatCartPrice(item.price, item.currency)} × {item.quantity} {item.unit}
                </div>
              </div>

              {/* Quantity controls with long-press auto-repeat */}
              <CartQtyControls item={item} cart={cart} />

              {/* Line total */}
              <div className="text-sm font-semibold text-right min-w-[56px]">
                {formatCartPrice(item.price * item.quantity, item.currency)}
              </div>

              {/* Remove this item — × button */}
              <button
                onClick={() => handleRemoveItem(item)}
                aria-label={t.remove_item}
                title={t.remove_item}
                className="flex-shrink-0 w-7 h-7 -mr-1 rounded-full text-tg-hint active:text-red-500 active:bg-red-500/15 flex items-center justify-center text-xl leading-none no-callout"
              >
                ×
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Totals by currency */}
      <div className="bg-tg-secondary rounded-xl p-4 mb-4">
        <div className="text-sm font-semibold text-tg-hint uppercase mb-2">{t.total}</div>
        {Object.entries(cart.totals).map(([currency, total]) => (
          <div key={currency} className="flex justify-between items-center py-1">
            <span className="text-base font-semibold">{t.total} ({currency})</span>
            <span className="text-lg font-bold text-tg-link">
              {formatCartPrice(total, currency)}
            </span>
          </div>
        ))}

        {/* Total weight — helps users decide on delivery (own car vs ours) */}
        {cart.totalWeight > 0 && (
          <div className="flex justify-between items-center py-1 mt-1 border-t border-tg-hint/15 pt-2">
            <span className="text-base font-semibold">{t.total_weight}</span>
            <span className="text-lg font-bold text-tg-text">
              {cart.itemsMissingWeight > 0 ? '~' : ''}{cart.totalWeight.toFixed(1)} kg
            </span>
          </div>
        )}
        {cart.itemsMissingWeight > 0 && (
          <div className="text-[11px] text-tg-hint italic mt-1">
            {t.weight_missing_some.replace('{count}', cart.itemsMissingWeight)}
          </div>
        )}

        <div className="text-xs text-tg-hint mt-2">
          {cart.totalCount} {t.products_count}
        </div>
      </div>

      {/* Clear cart */}
      <button
        onClick={cart.clearCart}
        className="w-full text-center text-sm text-red-400 py-2 mb-4"
      >
        {t.clear_cart}
      </button>

      {/* Export buttons */}
      {exported ? (
        <div className="text-center py-4">
          <div className="text-3xl mb-2">{exported === 'telegram' ? '✅' : '📥'}</div>
          <div className="text-base font-medium">
            {exported === 'telegram'
              ? 'Hisobot Telegram chatga yuborildi!'
              : t.order_ready}
          </div>
          {exported === 'telegram' && (
            <div className="text-sm text-tg-hint mt-2">
              Bot bilan chatni oching — fayl o'sha yerda
            </div>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          <button
            onClick={() => setPreviewFormat('pdf')}
            disabled={exporting}
            className="w-full bg-tg-button text-tg-button-text rounded-xl py-3 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
          >
            📄 Hisobot yuborish (PDF)
          </button>
          <button
            onClick={() => setPreviewFormat('xlsx')}
            disabled={exporting}
            className="w-full bg-green-600 text-white rounded-xl py-3 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
          >
            📊 Hisobot yuborish (Excel)
          </button>
        </div>
      )}

      {/* Undo toast — fixed at bottom, ~4s window after × delete */}
      {removedItem && (
        <div
          className="fixed left-1/2 -translate-x-1/2 z-[200] flex items-center gap-3 px-4 py-3 rounded-full shadow-2xl bg-black/85 text-white text-sm max-w-[92%]"
          style={{ bottom: 'calc(env(safe-area-inset-bottom, 0px) + 16px)' }}
          role="status"
        >
          <span className="truncate">
            {t.item_removed}: <span className="font-medium">{removedItem.name_display || removedItem.name}</span>
          </span>
          <button
            onClick={handleUndoRemove}
            className="flex-shrink-0 font-semibold uppercase text-xs tracking-wide px-3 py-1.5 rounded-full bg-white/15 active:bg-white/25"
          >
            {t.undo}
          </button>
        </div>
      )}

    </div>
  );
}
