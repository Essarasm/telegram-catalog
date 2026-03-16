import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchProducts, formatPrice, getPriceCurrency, getPriceValue, getImageUrl } from '../utils/api';
import t from '../i18n/uz.json';

export default function ProductsPage({ category, producer, searchQuery, cart, approved, onSelectProduct }) {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
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
    return <div className="text-center py-10 text-tg-hint">{t.no_products}</div>;
  }

  return (
    <div>
      {/* 2-column product card grid */}
      <div className="grid grid-cols-2 gap-2.5">
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
                {/* Product image */}
                <div className="w-full aspect-square bg-tg-bg flex items-center justify-center overflow-hidden">
                  {imgUrl ? (
                    <img src={imgUrl} alt="" className="w-full h-full object-cover" loading="lazy" />
                  ) : (
                    <span className="text-4xl opacity-20">📷</span>
                  )}
                </div>

                {/* Product info */}
                <div className="p-2.5">
                  <div className="text-xs font-medium leading-tight line-clamp-2 min-h-[2rem]">
                    {displayName}
                  </div>
                  {approved ? (
                    <div className="text-sm font-bold text-tg-link mt-1">
                      {priceStr}
                    </div>
                  ) : (
                    <div className="text-[10px] text-tg-hint mt-1 italic leading-tight">
                      Narxni bilish uchun bog'laning
                    </div>
                  )}
                </div>
              </div>

              {/* Add to cart button */}
              {approved && (
                <div className="px-2.5 pb-2.5 mt-auto">
                  {inCart ? (
                    <div className="flex items-center justify-between bg-tg-button rounded-lg px-2 py-1.5">
                      <button
                        onClick={() => cart.updateQuantity(product.id, inCart.quantity - 1)}
                        className="text-tg-button-text font-bold text-base w-7 text-center"
                      >
                        −
                      </button>
                      <span className="text-tg-button-text text-sm font-semibold">
                        {inCart.quantity}
                      </span>
                      <button
                        onClick={() => cart.updateQuantity(product.id, inCart.quantity + 1)}
                        className="text-tg-button-text font-bold text-base w-7 text-center"
                      >
                        +
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={() => cart.addItem({
                        ...product,
                        price: getPriceValue(product.price_usd, product.price_uzs),
                        currency: getPriceCurrency(product.price_usd, product.price_uzs),
                      })}
                      className="w-full bg-tg-button text-tg-button-text text-xs font-medium rounded-lg py-2 active:scale-95 transition-transform"
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

      {loading && <div className="text-center py-4 text-tg-hint">{t.loading}</div>}
    </div>
  );
}
