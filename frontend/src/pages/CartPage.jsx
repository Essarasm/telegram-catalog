import { useState, useRef, useEffect } from 'react';
import { formatCartPrice } from '../utils/api';
import t from '../i18n/uz.json';

const API_BASE = '/api';
const QUICK_QTYS = [1, 5, 10, 25, 50, 100];

export default function CartPage({ cart }) {
  const [exporting, setExporting] = useState(false);
  const [exported, setExported] = useState(false);
  const [editItem, setEditItem] = useState(null); // item being edited in bottom sheet
  const [editValue, setEditValue] = useState('');
  const inputRef = useRef(null);

  useEffect(() => {
    if (editItem && inputRef.current) {
      // Small delay to let the sheet animate in
      setTimeout(() => {
        inputRef.current?.focus();
        inputRef.current?.select();
      }, 100);
    }
  }, [editItem]);

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

  const openEditor = (item) => {
    setEditItem(item);
    setEditValue(String(item.quantity));
  };

  const confirmEdit = () => {
    if (!editItem) return;
    const val = parseInt(editValue, 10);
    if (val > 0) {
      cart.updateQuantity(editItem.id, val);
    } else {
      cart.removeItem(editItem.id);
    }
    setEditItem(null);
  };

  const applyQuickQty = (qty) => {
    if (!editItem) return;
    cart.updateQuantity(editItem.id, qty);
    setEditItem(null);
  };

  const removeFromSheet = () => {
    if (!editItem) return;
    cart.removeItem(editItem.id);
    setEditItem(null);
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

              {/* Quantity controls — simpler: −, tappable qty, + */}
              <div className="flex items-center gap-1.5">
                <button
                  onClick={() => cart.updateQuantity(item.id, item.quantity - 1)}
                  className="w-9 h-9 rounded-full bg-tg-button text-tg-button-text flex items-center justify-center font-bold text-lg"
                >
                  −
                </button>

                {/* Tappable quantity — opens bottom sheet */}
                <button
                  onClick={() => openEditor(item)}
                  className="text-sm font-bold min-w-[40px] text-center py-1.5 px-2 rounded-lg border-2 border-tg-link/40 active:bg-tg-link/10"
                  style={{ color: 'var(--tg-theme-link-color)' }}
                >
                  {item.quantity}
                </button>

                <button
                  onClick={() => cart.updateQuantity(item.id, item.quantity + 1)}
                  className="w-9 h-9 rounded-full bg-tg-button text-tg-button-text flex items-center justify-center font-bold text-lg"
                >
                  +
                </button>
              </div>

              {/* Line total */}
              <div className="text-sm font-semibold text-right min-w-[60px]">
                {formatCartPrice(item.price * item.quantity, item.currency)}
              </div>
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

      {/* Bottom-sheet quantity editor */}
      {editItem && (
        <>
          <div
            className="fixed inset-0 bg-black/40 z-[100]"
            onClick={() => setEditItem(null)}
          />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl"
            style={{ maxHeight: '60vh' }}
          >
            {/* Handle bar */}
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />

            <div className="text-center mb-4">
              <div className="text-base font-semibold truncate px-4">{editItem.name_display || editItem.name}</div>
              <div className="text-xs text-tg-hint mt-1">
                {formatCartPrice(editItem.price, editItem.currency)} / {editItem.unit}
              </div>
            </div>

            {/* Quick preset grid — large buttons */}
            <div className="grid grid-cols-3 gap-2.5 mb-5">
              {QUICK_QTYS.map(q => (
                <button
                  key={q}
                  onClick={() => applyQuickQty(q)}
                  className={`py-3.5 rounded-xl text-lg font-bold transition-colors ${
                    editItem.quantity === q
                      ? 'bg-tg-button text-tg-button-text'
                      : 'bg-tg-secondary text-tg-text active:bg-tg-button active:text-tg-button-text'
                  }`}
                >
                  {q}
                </button>
              ))}
            </div>

            {/* Custom input */}
            <div className="flex items-center gap-2 mb-4">
              <input
                ref={inputRef}
                type="number"
                inputMode="numeric"
                min="0"
                max="9999"
                placeholder="Boshqa son..."
                value={editValue}
                onChange={(e) => setEditValue(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter') confirmEdit(); }}
                className="flex-1 rounded-xl px-4 py-3.5 text-lg font-semibold text-center outline-none border-2 border-tg-hint/30 focus:border-tg-link"
                style={{ color: 'var(--tg-theme-text-color)', backgroundColor: 'var(--tg-theme-secondary-bg-color)' }}
              />
              <button
                onClick={confirmEdit}
                className="bg-tg-button text-tg-button-text rounded-xl px-6 py-3.5 font-bold text-lg"
              >
                ✓
              </button>
            </div>

            {/* Remove item */}
            <button
              onClick={removeFromSheet}
              className="w-full text-red-400 text-sm font-medium py-2"
            >
              O'chirish
            </button>
          </div>
        </>
      )}
    </div>
  );
}
