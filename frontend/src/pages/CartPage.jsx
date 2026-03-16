import { useState, useRef, useEffect } from 'react';
import { exportOrder, formatCartPrice } from '../utils/api';
import t from '../i18n/uz.json';

const API_BASE = '/api';
const QUICK_QTYS = [5, 10, 25, 50, 100];

export default function CartPage({ cart }) {
  const [exporting, setExporting] = useState(false);
  const [exported, setExported] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [editValue, setEditValue] = useState('');
  const inputRef = useRef(null);

  useEffect(() => {
    if (editingId && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [editingId]);

  const handleExport = async (format) => {
    setExporting(true);
    try {
      const tgUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
      const clientName = tgUser
        ? `${tgUser.first_name || ''} ${tgUser.last_name || ''}`.trim()
        : '';
      const telegramId = tgUser?.id || 0;

      // Build the export URL with query params so we can open it directly
      const params = new URLSearchParams({
        format,
        client_name: clientName,
        telegram_id: telegramId,
      });
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

      const tgApp = window.Telegram?.WebApp;
      const downloadToken = res.headers.get('X-Download-Token');

      // Android Telegram WebView: open a real server URL in system browser
      if (tgApp?.openLink && downloadToken) {
        const origin = window.location.origin;
        const downloadUrl = `${origin}${API_BASE}/export/download/${downloadToken}`;
        tgApp.openLink(downloadUrl);
      } else {
        // Desktop / iOS — standard blob download
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
      setExported(true);
    } catch (err) {
      console.error('Export failed:', err);
    }
    setExporting(false);
  };

  const startEdit = (item) => {
    setEditingId(item.id);
    setEditValue(String(item.quantity));
  };

  const confirmEdit = (itemId) => {
    const val = parseInt(editValue, 10);
    if (val > 0) {
      cart.updateQuantity(itemId, val);
    } else if (val === 0 || editValue === '' || editValue === '0') {
      cart.removeItem(itemId);
    }
    setEditingId(null);
  };

  const applyQuickQty = (itemId, qty) => {
    cart.updateQuantity(itemId, qty);
    setEditingId(null);
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

  return (
    <div>
      {/* Cart items */}
      <div className="space-y-2 mb-6">
        {cart.items.map(item => (
          <div key={item.id} className="bg-tg-secondary rounded-xl p-3">
            <div className="flex items-center gap-3">
              <div className="flex-1 min-w-0">
                <div className="text-sm font-medium leading-tight truncate">{item.name_display || item.name}</div>
                <div className="text-xs text-tg-hint mt-0.5">
                  {formatCartPrice(item.price, item.currency)} × {item.quantity} {item.unit}
                </div>
              </div>

              {/* Quantity controls */}
              <div className="flex items-center gap-2">
                <button
                  onClick={() => cart.updateQuantity(item.id, item.quantity - 1)}
                  className="w-8 h-8 rounded-full bg-tg-button text-tg-button-text flex items-center justify-center font-bold text-sm"
                >
                  −
                </button>

                {/* Tappable quantity — opens edit mode */}
                <button
                  onClick={() => startEdit(item)}
                  className="text-sm font-semibold min-w-[32px] text-center py-1 px-1 rounded-lg bg-white/10 border border-tg-hint/30 active:bg-white/20"
                >
                  {item.quantity}
                </button>

                <button
                  onClick={() => cart.updateQuantity(item.id, item.quantity + 1)}
                  className="w-8 h-8 rounded-full bg-tg-button text-tg-button-text flex items-center justify-center font-bold text-sm"
                >
                  +
                </button>
              </div>

              {/* Line total */}
              <div className="text-sm font-semibold text-right min-w-[70px]">
                {formatCartPrice(item.price * item.quantity, item.currency)}
              </div>

              {/* Remove */}
              <button
                onClick={() => cart.removeItem(item.id)}
                className="text-red-400 text-lg ml-1"
              >
                ✕
              </button>
            </div>

            {/* Expanded edit panel */}
            {editingId === item.id && (
              <div className="mt-3 pt-3 border-t border-tg-hint/20">
                <div className="flex items-center gap-2 mb-2">
                  <input
                    ref={inputRef}
                    type="number"
                    inputMode="numeric"
                    min="0"
                    max="9999"
                    value={editValue}
                    onChange={(e) => setEditValue(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') confirmEdit(item.id); }}
                    className="flex-1 bg-white/10 border border-tg-hint/40 rounded-lg px-3 py-2 text-center text-base font-semibold outline-none focus:border-tg-link"
                    style={{ color: 'var(--tg-theme-text-color)', backgroundColor: 'var(--tg-theme-bg-color)' }}
                  />
                  <button
                    onClick={() => confirmEdit(item.id)}
                    className="bg-tg-button text-tg-button-text rounded-lg px-4 py-2 text-sm font-semibold"
                  >
                    ✓
                  </button>
                  <button
                    onClick={() => setEditingId(null)}
                    className="text-tg-hint rounded-lg px-3 py-2 text-sm"
                  >
                    ✕
                  </button>
                </div>
                {/* Quick preset buttons */}
                <div className="flex gap-1.5 flex-wrap">
                  {QUICK_QTYS.map(q => (
                    <button
                      key={q}
                      onClick={() => applyQuickQty(item.id, q)}
                      className="px-3 py-1 rounded-full text-xs font-medium border border-tg-hint/30 active:bg-tg-button active:text-tg-button-text transition-colors"
                      style={{ color: 'var(--tg-theme-link-color)' }}
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}
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
          <div className="text-3xl mb-2">✅</div>
          <div className="text-base font-medium">{t.order_ready}</div>
        </div>
      ) : (
        <div className="space-y-3">
          <button
            onClick={() => handleExport('pdf')}
            disabled={exporting}
            className="w-full bg-tg-button text-tg-button-text rounded-xl py-3 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
          >
            {exporting ? t.loading : `📄 ${t.download_pdf}`}
          </button>
          <button
            onClick={() => handleExport('xlsx')}
            disabled={exporting}
            className="w-full bg-green-600 text-white rounded-xl py-3 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
          >
            {exporting ? t.loading : `📊 ${t.download_excel}`}
          </button>
        </div>
      )}
    </div>
  );
}
