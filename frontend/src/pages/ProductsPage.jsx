import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchProducts, formatPrice, getImageUrl } from '../utils/api';
import t from '../i18n/uz.json';

export default function ProductsPage({ category, searchQuery, cart }) {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const observer = useRef();

  const loadProducts = useCallback(async (pageNum, reset = false) => {
    setLoading(true);
    const data = await fetchProducts({
      categoryId: category?.id,
      search: searchQuery,
      page: pageNum,
      limit: 30,
    });
    setProducts(prev => reset ? data.items : [...prev, ...data.items]);
    setHasMore(pageNum < data.pages);
    setLoading(false);
  }, [category?.id, searchQuery]);

  useEffect(() => {
    setPage(1);
    setProducts([]);
    loadProducts(1, true);
  }, [category?.id, searchQuery, loadProducts]);

  // Infinite scroll
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

  if (!loading && products.length === 0) {
    return <div className="text-center py-10 text-tg-hint">{t.no_products}</div>;
  }

  return (
    <div className="space-y-2">
      {products.map((product, idx) => {
        const inCart = isInCart(product.id);
        const imgUrl = getImageUrl(product);
        const isLast = idx === products.length - 1;

        return (
          <div
            key={product.id}
            ref={isLast ? lastRef : null}
            className="bg-tg-secondary rounded-xl p-3 flex gap-3 items-center"
          >
            {/* Image or placeholder */}
            <div className="w-14 h-14 rounded-lg bg-gray-200 flex-shrink-0 flex items-center justify-center overflow-hidden">
              {imgUrl ? (
                <img src={imgUrl} alt="" className="w-full h-full object-cover" />
              ) : (
                <span className="text-2xl opacity-30">📷</span>
              )}
            </div>

            {/* Info */}
            <div className="flex-1 min-w-0">
              <div className="text-sm font-medium leading-tight truncate">{product.name}</div>
              <div className="text-xs text-tg-hint mt-0.5">
                {product.unit} · {product.weight ? `${product.weight} kg` : ''}
              </div>
              <div className="text-sm font-semibold text-tg-link mt-1">
                {formatPrice(product.price, product.currency)}
              </div>
            </div>

            {/* Add/quantity controls */}
            <div className="flex-shrink-0">
              {inCart ? (
                <div className="flex items-center gap-2 bg-tg-button rounded-lg px-2 py-1">
                  <button
                    onClick={() => cart.updateQuantity(product.id, inCart.quantity - 1)}
                    className="text-tg-button-text font-bold text-lg w-6 text-center"
                  >
                    −
                  </button>
                  <span className="text-tg-button-text text-sm font-semibold min-w-[20px] text-center">
                    {inCart.quantity}
                  </span>
                  <button
                    onClick={() => cart.updateQuantity(product.id, inCart.quantity + 1)}
                    className="text-tg-button-text font-bold text-lg w-6 text-center"
                  >
                    +
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => cart.addItem(product)}
                  className="bg-tg-button text-tg-button-text text-xs font-medium rounded-lg px-3 py-2 active:scale-95 transition-transform"
                >
                  + {t.add_to_cart}
                </button>
              )}
            </div>
          </div>
        );
      })}

      {loading && <div className="text-center py-4 text-tg-hint">{t.loading}</div>}
    </div>
  );
}
