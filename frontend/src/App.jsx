import { useState, useEffect } from 'react';
import { useCart } from './hooks/useCart';
import CatalogPage from './pages/CatalogPage';
import ProducersPage from './pages/ProducersPage';
import ProductsPage from './pages/ProductsPage';
import CartPage from './pages/CartPage';
import ProductDetailPage from './pages/ProductDetailPage';
import RegisterPage from './pages/RegisterPage';
import t from './i18n/uz.json';

const APP_VERSION = 'v15';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

export default function App() {
  const [page, setPage] = useState('catalog');
  const [selectedCategory, setSelectedCategory] = useState(null);
  const [selectedProducer, setSelectedProducer] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [appError, setAppError] = useState(null);
  const [registered, setRegistered] = useState(null); // null = checking, true/false
  const [approved, setApproved] = useState(false); // false = no prices, true = full access
  const cart = useCart();

  // Check registration on mount
  useEffect(() => {
    const uid = getTelegramUserId();
    if (!uid) {
      // Outside Telegram — skip registration gate, show prices for dev
      setRegistered(true);
      setApproved(true);
      return;
    }
    fetch(`/api/users/check?telegram_id=${uid}`)
      .then(r => r.json())
      .then(data => {
        setRegistered(data.registered);
        setApproved(data.approved || false);
      })
      .catch(() => { setRegistered(true); setApproved(false); });
  }, []);

  const navigateTo = (p, data) => {
    try {
      if (p === 'producers' && data) {
        setSelectedCategory(data);
        setSelectedProducer(null);
        setSearchQuery('');
      }
      if (p === 'products' && data) {
        setSelectedProducer(data);
        setSearchQuery('');
      }
      if (p === 'product_detail' && data) {
        setSelectedProduct(data);
      }
      if (p === 'search' && data) {
        setSearchQuery(data);
        setSelectedCategory(null);
        setSelectedProducer(null);
        p = 'products';
      }
      setPage(p);
    } catch (err) {
      setAppError(`Nav error: ${err.message}`);
    }
  };

  const goBack = () => {
    if (page === 'cart') setPage(selectedProducer ? 'products' : selectedCategory ? 'producers' : 'catalog');
    else if (page === 'product_detail') { setPage('products'); setSelectedProduct(null); }
    else if (page === 'products' && searchQuery) { setPage('catalog'); setSearchQuery(''); }
    else if (page === 'products') setPage('producers');
    else if (page === 'producers') { setPage('catalog'); setSelectedCategory(null); }
    else setPage('catalog');
  };

  const getTitle = () => {
    if (page === 'catalog') return t.app_title;
    if (page === 'producers') return selectedCategory?.name || t.producers;
    if (page === 'products' && searchQuery) return t.search_results;
    if (page === 'products') return selectedProducer?.name || t.all_products;
    if (page === 'product_detail') return selectedProducer?.name || t.all_products;
    if (page === 'cart') return t.cart;
    return t.app_title;
  };

  // Expand Telegram Mini App
  try {
    if (window.Telegram?.WebApp) {
      window.Telegram.WebApp.expand();
      window.Telegram.WebApp.ready();
    }
  } catch (e) {
    // ignore Telegram API errors
  }

  // Loading state while checking registration
  if (registered === null) {
    return (
      <div className="min-h-screen bg-tg-bg text-tg-text flex items-center justify-center">
        <div className="text-tg-hint">Yuklanmoqda...</div>
      </div>
    );
  }

  // Registration gate — only phone is required
  if (registered === false) {
    return (
      <div className="min-h-screen bg-tg-bg text-tg-text">
        <RegisterPage onRegistered={(isApproved) => {
          setRegistered(true);
          setApproved(isApproved);
        }} />
      </div>
    );
  }

  // Everyone gets into the catalog — approved prop controls price visibility
  return (
    <div className="min-h-screen bg-tg-bg text-tg-text pb-20">
      {/* Top bar */}
      <header className="sticky top-0 z-50 bg-tg-bg border-b border-tg-hint/20 px-4 py-3">
        <div className="flex items-center justify-between">
          {page !== 'catalog' && (
            <button onClick={goBack} className="text-tg-link font-medium text-sm">
              ← {t.back}
            </button>
          )}
          <h1 className="text-base font-semibold flex-1 text-center truncate">
            {getTitle()}
          </h1>
          {approved && (
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
          )}
        </div>
      </header>

      {/* Version + debug bar */}
      <div className="px-4 py-1 text-[10px] text-gray-400 flex justify-between">
        <span>{APP_VERSION} | page={page}</span>
        <span id="js-error-log" className="text-red-400 truncate max-w-[200px]">
          {appError || ''}
        </span>
      </div>

      {/* Content */}
      <main className="px-4 py-3">
        {page === 'catalog' && (
          <CatalogPage
            onSelectCategory={(cat) => navigateTo('producers', cat)}
            onSearch={(q) => navigateTo('search', q)}
          />
        )}
        {page === 'producers' && (
          <ProducersPage
            category={selectedCategory}
            onSelectProducer={(prod) => navigateTo('products', prod)}
          />
        )}
        {page === 'products' && (
          <ProductsPage
            category={selectedCategory}
            producer={selectedProducer}
            searchQuery={searchQuery}
            cart={cart}
            approved={approved}
            onSelectProduct={(product) => navigateTo('product_detail', product)}
          />
        )}
        {page === 'product_detail' && selectedProduct && (
          <ProductDetailPage
            product={selectedProduct}
            producer={selectedProducer}
            cart={cart}
            approved={approved}
            onBack={goBack}
          />
        )}
        {page === 'cart' && (
          <CartPage cart={cart} approved={approved} />
        )}
      </main>
    </div>
  );
}
