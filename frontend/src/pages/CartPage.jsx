import { useState } from 'react';
import { exportOrder, formatPrice } from '../utils/api';
import t from '../i18n/uz.json';

export default function CartPage({ cart }) {
  const [exporting, setExporting] = useState(false);
  const [exported, setExported] = useState(false);

  const handleExport = async (format) => {
    setExporting(true);
    try {
      // Get Telegram user name if available
      const tgUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
      const clientName = tgUser
        ? `${tgUser.first_name || ''} ${tgUser.last_name || ''}`.trim()
        : '';

      const blob = await exportOrder(cart.items, format, clientName);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `buyurtma.${format === 'xlsx' ? 'xlsx' : 'pdf'}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      setExported(true);
    } catch (err) {
      console.error('Export failed:', err);
    }
    setExporting(false);
  };

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
          <div key={item.id} className="bg-tg-secondary rounded-xl p-3 flex items-center gap-3">
            <div className="flex-1 min-w-0">
              <div className="text-sm font-medium leading-tight truncate">{item.name}</div>
              <div className="text-xs text-tg-hint mt-0.5">
                {formatPrice(item.price, item.currency)} × {item.quantity} {item.unit}
              </div>
            </div>

            {/* Quantity controls */}
            <div className="flex items-center gap-2">
              <button
                onClick={() => cart.updateQuantity(item.id, item.quantity - 1)}
                className="w-8 h-8 rounded-full bg-gray-200 flex items-center justify-center font-bold"
              >
                −
              </button>
              <span className="text-sm font-semibold min-w-[24px] text-center">{item.quantity}</span>
              <button
                onClick={() => cart.updateQuantity(item.id, item.quantity + 1)}
                className="w-8 h-8 rounded-full bg-gray-200 flex items-center justify-center font-bold"
              >
                +
              </button>
            </div>

            {/* Line total */}
            <div className="text-sm font-semibold text-right min-w-[70px]">
              {formatPrice(item.price * item.quantity, item.currency)}
            </div>

            {/* Remove */}
            <button
              onClick={() => cart.removeItem(item.id)}
              className="text-red-400 text-lg ml-1"
            >
              ✕
            </button>
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
              {formatPrice(total, currency)}
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
