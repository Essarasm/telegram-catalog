import { useState } from 'react';
import { useCart } from './hooks/useCart';
import CatalogPage from './pages/CatalogPage';
import ProductsPage from './pages/ProductsPage';
import CartPage from './pages/CartPage';
import t from './i18n/uz.json';

export default function App() {
  const [page, setPage] = useState('catalog');
  const [selectedCategory, setSelectedCategory] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const cart = useCart();

  const navigateTo = (p, data) => {
    if (p === 'products' && data) {
      setSelectedCategory(data);
      setSearchQuery('');
    }
    if (p === 'search' && data) {
      setSearchQuery(data);
      setSelectedCategory(null);
    }
    setPage(p === 'search' ? 'products' : p);
  };

  // Expand Telegram Mini App
  if (window.Telegram?.WebApp) {
    window.Telegram.WebApp.expand();
    window.Telegram.WebApp.ready();
  }

  return (
    <div className="min-h-screen bg-tg-bg text-tg-text pb-20">
      {/* Top bar */}
      <header className="sticky top-0 z-50 bg-tg-bg border-b border-gray-200 px-4 py-3">
        <div className="flex items-center justify-between">
          {page !== 'catalog' && (
            <button
              onClick={() => page === 'cart' ? setPage('catalog') : setPage('catalog')}
              className="text-tg-link font-medium text-sm"
            >
              ← {t.back}
            </button>
          )}
          <h1 className="text-base font-semibold flex-1 text-center truncate">
            {page === 'catalog' && t.app_title}
            {page === 'products' && (selectedCategory?.name || t.search_results)}
            {page === 'cart' && t.cart}
          </h1>
          <button
            onClick={() => navigateTo('cart')}
            className="relative text-xl ml-2"
          >
            🛒
            {cart.totalCount > 0 && (
              <span className="absolute -top-2 -right-2 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center">
                {cart.totalCount}
              </span>
            )}
          </button>
        </div>
      </header>

      {/* Content */}
      <main className="px-4 py-3">
        {page === 'catalog' && (
          <CatalogPage onSelectCategory={(cat) => navigateTo('products', cat)} onSearch={(q) => navigateTo('search', q)} />
        )}
        {page === 'products' && (
          <ProductsPage
            category={selectedCategory}
            searchQuery={searchQuery}
            cart={cart}
          />
        )}
        {page === 'cart' && (
          <CartPage cart={cart} />
        )}
      </main>
    </div>
  );
}
