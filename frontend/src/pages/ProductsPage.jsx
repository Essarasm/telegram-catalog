import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchProducts, formatPrice, getPriceCurrency, getPriceValue, getImageUrl } from '../utils/api';
import t from '../i18n/uz.json';

const WHOLESALE_QTYS = [1, 5, 10, 25, 50, 100];

export default function ProductsPage({ category, producer, searchQuery, cart, approved, onSelectProduct }) {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [qtyPickerId, setQtyPickerId] = useState(null);
  const [customQty, setCustomQty] = useState('');
  const observer = useRef();

  const loadProducts = useCallback(async (pageNum, reset = false) => {
    try {
      setLoading(true);
      const data = await fetchProducts({
        categoryId: category?.id,
        producerId: producer?.id,
        search: searchQuery,
        page: pageNum,
        limit: 30,
      });
      if (data && data.items) {
        setProducts(prev => reset ? data.items : [...prev, ...data.items]);
        setHasMore(pageNum < data.pages);
      } else {
        setError('Products API unexpected: ' + JSON.stringify(data).slice(0, 100));
      }
      setLoading(false);
    } catch (err) {
      setError('Fetch error: ' + (err.message || String(err)));
      setLoading(false);
    }
  }, [category?.id, producer?.id, searchQuery]);

  useEffect(() => {
    setPage(1);
    setProducts([]);
    setError(null);
    loadProducts(1, true);
  }, [category?.id, producer?.id, searchQuery, loadProducts]);

  const lastRef = useCallback(node => {
    if (loading) return;
    if (observer.current) observer.current.disconnect();
    observer.current = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting && hasMore) {
        const nextPage = page + 1;
        setPage(nextPage);
        loadProducts(nextPage);
      }
    });
    if (node) observer.current.observe(node);
  }, [loading, hasMore, page, loadProducts]);

  const isInCart = (id) => cart.items.find(i => i.id === id);

  if (error) {
    return (
      <div className="text-center py-10">
        <div className="text-red-500 text-sm font-mono mb-2">ProductsPage Error:</div>
        <div className="text-red-400 text-xs font-mono">{error}</div>
      </div>
    );
  }

  if (!loading && products.length === 0) {
    return <div className="text-center py-10 text-tg-hint text-base">{t.no_products}</div>;
  }

  return (
    <div>
      {/* 2-column product card grid — sized for ~4 cards visible per screen */}
      <div className="grid grid-cols-2 gap-3">
        {products.map((product, idx) => {
          const inCart = isInCart(product.id);
          const imgUrl = getImageUrl(product);
          const isLast = idx === products.length - 1;
          const displayName = product.name_display || product.name;
          const priceStr = approved ? formatPrice(product.price_usd, product.price_uzs) : null;

          return (
            <div
              key={product.id}
              ref={isLast ? lastRef : null}
              className="bg-tg-secondary rounded-xl overflow-hidden flex flex-col"
            >
              {/* Clickable card area */}
              <div
                className="cursor-pointer active:opacity-80 transition-opacity"
                onClick={() => onSelectProduct && onSelectProduct(product)}
              >
                {/* Product image — larger */}
                <div className="w-full aspect-square bg-tg-bg flex items-center justify-center overflow-hidden">
                  {imgUrl ? (
                    <img src={imgUrl} alt="" className="w-full h-full object-cover" loading="lazy" />
                  ) : (
                    <span className="text-5xl opacity-20">📷</span>
                  )}
                </div>

                {/* Product info — larger text */}
                <div className="p-3">
                  <div className="text-sm font-medium leading-snug line-clamp-2 min-h-[2.5rem]">
                    {displayName}
                  </div>
                  {approved ? (
                    <div className="text-base font-bold text-tg-link mt-1.5">
                      {priceStr}
                    </div>
                  ) : (
                    <div className="text-xs text-tg-hint mt-1.5 italic leading-tight">
                      Narxni bilish uchun bog'laning
                    </div>
                  )}
                </div>
              </div>

              {/* Add to cart / quantity controls */}
              {approved && (
                <div className="px-3 pb-3 mt-auto">
                  {inCart ? (
                    <div className="flex items-center justify-between bg-tg-button rounded-lg px-2 py-2">
                      <button
                        onClick={(e) => { e.stopPropagation(); cart.updateQuantity(product.id, inCart.quantity - 1); }}
                        className="text-tg-button-text font-bold text-lg w-10 h-9 flex items-center justify-center"
                      >
                        −
                      </button>
                      <button
                        onClick={(e) => { e.stopPropagation(); setQtyPickerId(qtyPickerId === product.id ? null : product.id); setCustomQty(String(inCart.quantity)); }}
                        className="text-tg-button-text text-base font-bold px-2 py-1 rounded-md bg-white/15 min-w-[36px] text-center"
                      >
                        {inCart.quantity}
                      </button>
                      <button
                        onClick={(e) => { e.stopPropagation(); cart.updateQuantity(product.id, inCart.quantity + 1); }}
                        className="text-tg-button-text font-bold text-lg w-10 h-9 flex items-center justify-center"
                      >
                        +
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={(e) => { e.stopPropagation(); cart.addItem({
                        ...product,
                        price: getPriceValue(product.price_usd, product.price_uzs),
                        currency: getPriceCurrency(product.price_usd, product.price_uzs),
                      }); }}
                      className="w-full bg-tg-button text-tg-button-text text-sm font-semibold rounded-lg py-2.5 active:scale-95 transition-transform"
                    >
                      + {t.add_to_cart}
                    </button>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {loading && <div className="text-center py-4 text-tg-hint text-base">{t.loading}</div>}

      {/* Bottom-sheet quantity picker */}
      {qtyPickerId && (() => {
        const product = products.find(p => p.id === qtyPickerId);
        const inCart = cart.items.find(i => i.id === qtyPickerId);
        if (!product || !inCart) return null;
        return (
          <>
            <div
              className="fixed inset-0 bg-black/40 z-[100]"
              onClick={() => setQtyPickerId(null)}
            />
            <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl"
              style={{ maxHeight: '60vh' }}
            >
              <div className="text-center mb-4">
                <div className="text-sm font-semibold truncate">{product.name_display || product.name}</div>
                <div className="text-xs text-tg-hint mt-1">Miqdorni tanlang</div>
              </div>
              <div className="grid grid-cols-3 gap-2 mb-4">
                {WHOLESALE_QTYS.map(q => (
                  <button
                    key={q}
                    onClick={() => { cart.updateQuantity(qtyPickerId, q); setQtyPickerId(null); }}
                    className={`py-3 rounded-xl text-base font-bold transition-colors ${
                      inCart.quantity === q
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
                      if (v > 0) { cart.updateQuantity(qtyPickerId, v); setQtyPickerId(null); }
                    }
                  }}
                  className="flex-1 rounded-xl px-4 py-3 text-base font-semibold text-center outline-none border border-tg-hint/30 focus:border-tg-link"
                  style={{ color: 'var(--tg-theme-text-color)', backgroundColor: 'var(--tg-theme-secondary-bg-color)' }}
                />
                <button
                  onClick={() => {
                    const v = parseInt(customQty, 10);
                    if (v > 0) { cart.updateQuantity(qtyPickerId, v); setQtyPickerId(null); }
                  }}
                  className="bg-tg-button text-tg-button-text rounded-xl px-5 py-3 font-bold text-base"
                >
                  ✓
                </button>
              </div>
              <button
                onClick={() => { cart.removeItem(qtyPickerId); setQtyPickerId(null); }}
                className="w-full text-red-400 text-sm font-medium py-2"
              >
                O'chirish
              </button>
            </div>
          </>
        );
      })()}
    </div>
  );
}
