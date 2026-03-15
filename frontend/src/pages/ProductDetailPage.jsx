import { getImageUrl, formatPrice } from '../utils/api';
import t from '../i18n/uz.json';

export default function ProductDetailPage({ product, producer, cart, onBack }) {
  const imgUrl = getImageUrl(product);
  const displayName = product.name_display || product.name;
  const priceStr = formatPrice(product.price_usd, product.price_uzs);
  const inCart = cart.items.find(i => i.id === product.id);

  const getPriceValue = () => {
    if (product.price_usd && product.price_usd > 0) return product.price_usd;
    return product.price_uzs || 0;
  };
  const getCurrency = () => {
    if (product.price_usd && product.price_usd > 0) return 'USD';
    return 'UZS';
  };

  return (
    <div className="space-y-4">
      {/* Large product image */}
      <div className="w-full aspect-square bg-gray-100 rounded-2xl overflow-hidden flex items-center justify-center">
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

      {/* Price */}
      <div className="text-2xl font-bold text-tg-link">
        {priceStr}
      </div>

      {/* Add to cart / quantity controls */}
      <div className="pt-2">
        {inCart ? (
          <div className="flex items-center justify-center gap-4 bg-tg-secondary rounded-xl py-3">
            <button
              onClick={() => cart.updateQuantity(product.id, inCart.quantity - 1)}
              className="bg-tg-button text-tg-button-text font-bold text-xl w-10 h-10 rounded-full flex items-center justify-center"
            >
              −
            </button>
            <span className="text-xl font-semibold min-w-[40px] text-center">
              {inCart.quantity}
            </span>
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
    </div>
  );
}
