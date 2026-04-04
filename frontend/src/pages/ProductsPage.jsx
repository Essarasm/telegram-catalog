import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchProducts, formatPrice, getPriceCurrency, getPriceValue, getImageUrl, submitProductRequest, logSearchClick, fetchDidYouMean } from '../utils/api';
import { useLongPress } from '../hooks/useLongPress';
import t from '../i18n/uz.json';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

function DidYouMean({ query, onSuggestionClick }) {
  const [suggestions, setSuggestions] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetchDidYouMean(query).then(results => {
      if (!cancelled) {
        setSuggestions(results);
        setLoading(false);
      }
    });
    return () => { cancelled = true; };
  }, [query]);

  if (loading || suggestions.length === 0) return null;

  return (
    <div className="mb-4 bg-tg-secondary rounded-xl p-3">
      <div className="text-sm text-tg-hint mb-2">Balki siz qidirdingiz:</div>
      <div className="flex flex-wrap gap-2">
        {suggestions.map((s, i) => (
          <button
            key={i}
            onClick={() => onSuggestionClick(s.text)}
            className="bg-tg-button/15 text-tg-link text-sm font-medium rounded-lg px-3 py-1.5 active:scale-95 transition-transform"
          >
            {s.text}
          </button>
        ))}
      </div>
    </div>
  );
}

function ProductsEmptyState({ searchQuery, onSuggestionClick }) {
  const [requestText, setRequestText] = useState('');
  const [sending, setSending] = useState(false);
  const [sent, setSent] = useState(false);

  const handleSubmit = async () => {
    if (!requestText.trim() || sending) return;
    setSending(true);
    try {
      await submitProductRequest({ telegramId: getTelegramUserId(), requestText: requestText.trim() });
      setSent(true);
    } catch (e) { /* silent */ }
    setSending(false);
  };

  return (
    <div className="text-center py-8">
      {/* "Did you mean?" suggestions for typo correction */}
      {searchQuery && (
        <DidYouMean query={searchQuery} onSuggestionClick={onSuggestionClick} />
      )}

      <div className="text-tg-hint text-base mb-6">{t.no_products}</div>

      <div className="bg-tg-secondary rounded-xl p-4 text-left">
        <div className="text-sm font-semibold mb-2">{t.cant_find}</div>
        {sent ? (
          <div className="text-center py-3">
            <div className="text-xl mb-1">✅</div>
            <div className="text-sm font-medium">{t.cant_find_sent}</div>
            <div className="text-xs text-tg-hint mt-1">{t.cant_find_thanks}</div>
          </div>
        ) : (
          <>
            <textarea
              placeholder={t.cant_find_placeholder}
              value={requestText}
              onChange={(e) => setRequestText(e.target.value)}
              rows={2}
              className="w-full rounded-xl px-4 py-3 text-sm outline-none border border-tg-hint/30 focus:border-tg-link resize-none mb-3"
              style={{ color: 'var(--tg-theme-text-color)', backgroundColor: 'var(--tg-theme-bg-color)' }}
            />
            <button
              onClick={handleSubmit}
              disabled={!requestText.trim() || sending}
              className={`w-full rounded-xl py-2.5 text-sm font-semibold transition-all ${
                requestText.trim() && !sending
                  ? 'bg-tg-button text-tg-button-text active:scale-[0.98]'
                  : 'bg-tg-hint/20 text-tg-hint'
              }`}
            >
              {sending ? t.loading : t.cant_find_submit}
            </button>
          </>
        )}
      </div>
    </div>
  );
}

/* Quantity stepper with long-press auto-repeat */
function QtyControls({ product, cart, inCart }) {
  const decBind = useLongPress(() => {
    const qty = cart.items.find(i => i.id === product.id)?.quantity || 1;
    if (qty <= 1) return false; // stop at 1 — require single tap to remove
    cart.updateQuantity(product.id, qty - 1);
  });
  const incBind = useLongPress(() => cart.updateQuantity(product.id, (cart.items.find(i => i.id === product.id)?.quantity || 0) + 1));

  return (
    <div className="flex items-center justify-between bg-tg-button rounded-lg px-2 py-2">
      <button
        onClick={(e) => { e.stopPropagation(); cart.updateQuantity(product.id, inCart.quantity - 1); }}
        {...decBind}
        className="text-tg-button-text font-bold text-lg w-10 h-9 flex items-center justify-center select-none"
      >
        −
      </button>
      <span className="text-tg-button-text text-base font-bold px-2 py-1 min-w-[36px] text-center select-none">
        {inCart.quantity}
      </span>
      <button
        onClick={(e) => { e.stopPropagation(); cart.updateQuantity(product.id, inCart.quantity + 1); }}
        {...incBind}
        className="text-tg-button-text font-bold text-lg w-10 h-9 flex items-center justify-center select-none"
      >
        +
      </button>
    </div>
  );
}

export default function ProductsPage({ category, producer, searchQuery, cart, approved, onSelectProduct, onSearch }) {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [isFuzzy, setIsFuzzy] = useState(false);
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
        telegramId: getTelegramUserId(),
      });
      if (data && data.items) {
        setProducts(prev => reset ? data.items : [...prev, ...data.items]);
        setHasMore(pageNum < data.pages);
        if (reset && data.fuzzy) {
          setIsFuzzy(true);
        } else if (reset) {
          setIsFuzzy(false);
        }
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
    setIsFuzzy(false);
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

  // Handle "Did you mean?" suggestion click
  const handleSuggestionClick = (text) => {
    if (onSearch) {
      onSearch(text);
    }
  };

  if (error) {
    return (
      <div className="text-center py-10">
        <div className="text-red-500 text-sm font-mono mb-2">ProductsPage Error:</div>
        <div className="text-red-400 text-xs font-mono">{error}</div>
      </div>
    );
  }

  if (!loading && products.length === 0) {
    return <ProductsEmptyState searchQuery={searchQuery} onSuggestionClick={handleSuggestionClick} />;
  }

  return (
    <div>
      {/* Fuzzy match indicator */}
      {isFuzzy && searchQuery && (
        <div className="mb-3 bg-tg-secondary rounded-xl px-3 py-2 flex items-center gap-2">
          <span className="text-sm">🔍</span>
          <span className="text-xs text-tg-hint">
            O'xshash natijalar: <span className="font-medium text-tg-text">"{searchQuery}"</span>
          </span>
        </div>
      )}

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
                onClick={() => {
                  if (searchQuery) {
                    logSearchClick({ telegramId: getTelegramUserId(), productId: product.id, action: 'click' });
                  }
                  onSelectProduct && onSelectProduct(product);
                }}
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
                  <div className="flex items-center gap-2 mt-1.5">
                    {approved ? (
                      <div className="text-base font-bold text-tg-link">
                        {priceStr}
                      </div>
                    ) : (
                      <div className="text-xs text-tg-hint italic leading-tight">
                        Narxni bilish uchun bog'laning
                      </div>
                    )}
                    {product.stock_status === 'out_of_stock' && (
                      <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded bg-red-500/15 text-red-500 whitespace-nowrap">
                        {t.stock_out_of_stock}
                      </span>
                    )}
                    {product.stock_status === 'low_stock' && (
                      <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-600 whitespace-nowrap">
                        {t.stock_low_stock}
                      </span>
                    )}
                  </div>
                </div>
              </div>

              {/* Add to cart / quantity controls */}
              {approved && (
                <div className="px-3 pb-3 mt-auto">
                  {inCart ? (
                    <QtyControls product={product} cart={cart} inCart={inCart} />
                  ) : (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        cart.addItem({
                          ...product,
                          price: getPriceValue(product.price_usd, product.price_uzs),
                          currency: getPriceCurrency(product.price_usd, product.price_uzs),
                        });
                        if (searchQuery) {
                          logSearchClick({ telegramId: getTelegramUserId(), productId: product.id, action: 'cart' });
                        }
                      }}
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
    </div>
  );
}
