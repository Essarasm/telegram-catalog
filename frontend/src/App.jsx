import { useState, useEffect, useCallback, useRef } from 'react';
import { useCart } from './hooks/useCart';
import CatalogPage from './pages/CatalogPage';
import ProducersPage from './pages/ProducersPage';
import ProductsPage from './pages/ProductsPage';
import CartPage from './pages/CartPage';
import ProductDetailPage from './pages/ProductDetailPage';
import RegisterPage from './pages/RegisterPage';
import t from './i18n/uz.json';

const APP_VERSION = 'v16.3';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

// ── Telegram CloudStorage helpers ──
// Persist registration data client-side so users don't need to
// re-register when the server DB is wiped during deployments.
function cloudSave(key, data) {
  try {
    window.Telegram?.WebApp?.CloudStorage?.setItem(key, JSON.stringify(data));
  } catch (e) { /* CloudStorage not available */ }
}

function cloudLoad(key) {
  return new Promise((resolve) => {
    try {
      const cs = window.Telegram?.WebApp?.CloudStorage;
      if (!cs) return resolve(null);
      cs.getItem(key, (err, val) => {
        if (err || !val) return resolve(null);
        try { resolve(JSON.parse(val)); } catch { resolve(null); }
      });
    } catch { resolve(null); }
  });
}

async function silentReRegister(uid) {
  const cached = await cloudLoad('reg_data');
  if (!cached || !cached.phone) return null;
  try {
    const res = await fetch('/api/users/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        telegram_id: uid,
        phone: cached.phone,
        first_name: cached.firstName || '',
        last_name: cached.lastName || '',
        username: cached.username || '',
      }),
    });
    return await res.json();
  } catch { return null; }
}

export default function App() {
  const [page, setPage] = useState('catalog');
  const [selectedCategory, setSelectedCategory] = useState(null);
  const [selectedProducer, setSelectedProducer] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [appError, setAppError] = useState(null);
  const [registered, setRegistered] = useState(null);
  const [approved, setApproved] = useState(false);
  const cart = useCart();
  const goBackRef = useRef(null);

  const checkApproval = useCallback(() => {
    const uid = getTelegramUserId();
    if (!uid) return;
    fetch(`/api/users/check?telegram_id=${uid}`)
      .then(r => r.json())
      .then(data => {
        setRegistered(data.registered);
        setApproved(data.approved || false);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    const uid = getTelegramUserId();
    if (!uid) {
      setRegistered(true);
      setApproved(true);
      return;
    }

    // Check server first, then fall back to CloudStorage cache
    fetch(`/api/users/check?telegram_id=${uid}`)
      .then(r => r.json())
      .then(async (data) => {
        if (data.registered) {
          // Server recognizes user — cache to CloudStorage for future resilience
          if (data.phone) {
            cloudSave('reg_data', {
              phone: data.phone,
              firstName: data.first_name || '',
              lastName: '',
              username: '',
            });
          }
          setRegistered(true);
          setApproved(data.approved || false);
        } else {
          // Server lost user data — try silent re-registration from cache
          const result = await silentReRegister(uid);
          if (result && result.ok) {
            setRegistered(true);
            setApproved(result.approved || false);
          } else {
            // No cache or re-register failed — show RegisterPage
            setRegistered(false);
            setApproved(false);
          }
        }
      })
      .catch(() => { setRegistered(true); setApproved(false); });
  }, []);

  useEffect(() => {
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') checkApproval();
    };
    document.addEventListener('visibilitychange', handleVisibility);
    const tg = window.Telegram?.WebApp;
    if (tg?.onEvent) tg.onEvent('viewportChanged', checkApproval);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
      if (tg?.offEvent) tg.offEvent('viewportChanged', checkApproval);
    };
  }, [checkApproval]);

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

  const goBack = useCallback(() => {
    setPage(prev => {
      if (prev === 'cart') return selectedProducer ? 'products' : selectedCategory ? 'producers' : 'catalog';
      if (prev === 'product_detail') { setSelectedProduct(null); return 'products'; }
      if (prev === 'products' && searchQuery) { setSearchQuery(''); return 'catalog'; }
      if (prev === 'products') return 'producers';
      if (prev === 'producers') { setSelectedCategory(null); return 'catalog'; }
      return 'catalog';
    });
  }, [selectedProducer, selectedCategory, searchQuery]);

  // Keep ref in sync
  goBackRef.current = goBack;

  // Telegram native BackButton — show/hide based on page, handle clicks
  useEffect(() => {
    const tg = window.Telegram?.WebApp;
    const bb = tg?.BackButton;
    if (!bb) return;

    const handler = () => { goBackRef.current?.(); };

    if (page !== 'catalog') {
      bb.show();
      bb.onClick(handler);
    } else {
      bb.hide();
    }

    return () => {
      bb.offClick(handler);
    };
  }, [page]);

  const getTitle = () => {
    if (page === 'catalog') return t.app_title;
    if (page === 'producers') return selectedCategory?.name || t.producers;
    if (page === 'products' && searchQuery) return t.search_results;
    if (page === 'products') return selectedProducer?.name || t.all_products;
    if (page === 'product_detail') return selectedProducer?.name || t.all_products;
    if (page === 'cart') return t.cart;
    return t.app_title;
  };

  try {
    if (window.Telegram?.WebApp) {
      window.Telegram.WebApp.expand();
      window.Telegram.WebApp.ready();
      window.Telegram.WebApp.enableClosingConfirmation();
    }
  } catch (e) {}

  if (registered === null) {
    return (
      <div className="min-h-screen bg-tg-bg text-tg-text flex items-center justify-center">
        <div className="text-tg-hint">Yuklanmoqda...</div>
      </div>
    );
  }

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

  return (
    <div className="min-h-screen bg-tg-bg text-tg-text pb-20">
      {/* Compact header — title + cart only (back button is Telegram native) */}
      <header className="sticky top-0 z-50 bg-tg-bg border-b border-tg-hint/20">
        <div className="flex items-center justify-between h-11 px-4">
          <h1 className="text-base font-semibold truncate flex-1">
            {getTitle()}
          </h1>
          {approved && (
            <button
              onClick={() => navigateTo('cart')}
              className="relative text-xl ml-3 p-1"
            >
              🛒
              {cart.totalCount > 0 && (
                <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center">
                  {cart.totalCount}
                </span>
              )}
            </button>
          )}
        </div>
      </header>

      {/* Unapproved banner */}
      {!approved && page === 'catalog' && (
        <div className="mx-4 mt-2 bg-tg-secondary rounded-xl p-3 text-center">
          <div className="text-sm text-tg-hint mb-2">
            Narxlarni ko'rish uchun menejer bilan bog'laning
          </div>
          <div className="flex gap-2 justify-center">
            <a
              href="https://t.me/axmatov0902"
              target="_blank"
              rel="noopener noreferrer"
              className="bg-tg-button text-tg-button-text text-xs font-medium rounded-lg px-4 py-2"
            >
              Telegram orqali yozish
            </a>
            <button
              onClick={checkApproval}
              className="border border-tg-hint/30 text-tg-hint text-xs font-medium rounded-lg px-4 py-2"
            >
              Tekshirish ↻
            </button>
          </div>
        </div>
      )}

      {/* Content */}
      <main className="px-3 py-3">
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
