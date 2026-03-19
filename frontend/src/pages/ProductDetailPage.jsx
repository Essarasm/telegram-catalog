import { useState } from 'react';
import { getImageUrl, formatPrice } from '../utils/api';
import t from '../i18n/uz.json';

const WHOLESALE_QTYS = [6, 12, 15, 25, 36, 50];

export default function ProductDetailPage({ product, producer, cart, approved, onBack }) {
  const [showQtyPicker, setShowQtyPicker] = useState(false);
  const [customQty, setCustomQty] = useState('');
  const imgUrl = getImageUrl(product);
  const displayName = product.name_display || product.name;
  const priceStr = approved ? formatPrice(product.price_usd, product.price_uzs) : null;
  const inCart = cart.items.find(i => i.id === product.id);

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

  return (
    <div className="space-y-4">
      {/* Large product image */}
      <div className="w-full aspect-square bg-tg-secondary rounded-2xl overflow-hidden flex items-center justify-center">
        {imgUrl ? (
          <img src={imgUrl} alt={displayName} className="w-full h-full object-contain" />
        ) : (
          <span className="text-6xl opacity-20">📷</span>
        )}
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
      <div className="flex items-center gap-4 text-sm text-tg-hint">
        {product.unit && <span>{t.unit || 'Birlik'}: {product.unit}</span>}
        {product.weight ? <span>{product.weight} kg</span> : null}
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
    </div>
  );
}
