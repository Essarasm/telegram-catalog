import { useState, useEffect, useCallback, useRef } from 'react';
import { useCart } from './hooks/useCart';
import CatalogPage from './pages/CatalogPage';
import ProducersPage from './pages/ProducersPage';
import ProductsPage from './pages/ProductsPage';
import CartPage from './pages/CartPage';
import ProductDetailPage from './pages/ProductDetailPage';
import RegisterPage from './pages/RegisterPage';
import CabinetPage from './pages/CabinetPage';
import t from './i18n/uz.json';
import { cloudSave, cloudLoad } from './utils/cloudStorage';

const APP_VERSION = 'v17.0';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
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
            await cloudSave('reg_data', {
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
      if (prev === 'cabinet') return 'catalog';
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
    if (page === 'product_detail') return selectedProduct?.name || t.all_products;
    if (page === 'cart') return t.cart;
    if (page === 'cabinet') return t.cabinet;
    return t.app_title;
  };

  // Breadcrumb trail — shows where the user is in the catalog hierarchy
  const getBreadcrumbs = () => {
    const crumbs = [];
    if (page === 'catalog') return crumbs; // no breadcrumb on home

    // Always start with catalog root
    crumbs.push({ label: t.categories, action: () => { setSelectedCategory(null); setSelectedProducer(null); setSearchQuery(''); setPage('catalog'); } });

    if (page === 'producers' && selectedCategory) {
      crumbs.push({ label: selectedCategory.name });
    }
    if ((page === 'products' || page === 'product_detail') && !searchQuery) {
      if (selectedCategory) {
        crumbs.push({ label: selectedCategory.name, action: () => { setSelectedProducer(null); setPage('producers'); } });
      }
      if (selectedProducer) {
        crumbs.push({ label: selectedProducer.name, action: page === 'product_detail' ? () => { setSelectedProduct(null); setPage('products'); } : undefined });
      }
      if (page === 'product_detail' && selectedProduct) {
        crumbs.push({ label: selectedProduct.name });
      }
    }
    if ((page === 'products' || page === 'product_detail') && searchQuery) {
      crumbs.push({ label: `"${searchQuery}"` });
      if (page === 'product_detail' && selectedProduct) {
        crumbs.push({ label: selectedProduct.name });
      }
    }
    if (page === 'cart') {
      crumbs.push({ label: t.cart });
    }
    if (page === 'cabinet') {
      crumbs.push({ label: t.cabinet });
    }
    return crumbs;
  };

  // ── Telegram WebApp initialization ──
  const [isFullscreen, setIsFullscreen] = useState(false);
  useEffect(() => {
    try {
      const tg = window.Telegram?.WebApp;
      if (!tg) return;
      // Critical: signal the app is ready (this lets Telegram show it)
      tg.ready();
      tg.expand();
      // Defer non-critical calls so the app renders faster
      setTimeout(() => {
        try {
          tg.enableClosingConfirmation?.();
          tg.disableVerticalSwipes?.();
          tg.setHeaderColor?.('#000000');
          tg.setBottomBarColor?.('bg_color');
          // Request true fullscreen (Bot API 8.0+)
          if (tg.requestFullscreen) {
            tg.requestFullscreen();
            setIsFullscreen(true);
            tg.onEvent?.('fullscreenChanged', () => {
              setIsFullscreen(!!tg.isFullscreen);
            });
          }
        } catch (e) {}
      }, 50);
    } catch (e) {}
  }, []);

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
        <RegisterPage onRegistered={async (isApproved, regData) => {
          // Save to CloudStorage from parent as safety net
          // (RegisterPage also saves, but this ensures it's done)
          if (regData?.phone) {
            await cloudSave('reg_data', {
              phone: regData.phone,
              firstName: regData.firstName || '',
              lastName: regData.lastName || '',
              username: regData.username || '',
            });
          }
          setRegistered(true);
          setApproved(isApproved);
        }} />
      </div>
    );
  }

  // Safe area insets from Telegram:
  // - safeAreaInset.top = device hardware (notch / dynamic island)
  // - contentSafeAreaInset.top = Telegram UI controls (back button, ˅ dropdown, ⋯ menu)
  // These are additive — content must clear BOTH the notch AND Telegram's controls
  const safeTop = window.Telegram?.WebApp?.safeAreaInset?.top || 0;
  const contentSafeTop = window.Telegram?.WebApp?.contentSafeAreaInset?.top || 0;
  const topPad = isFullscreen ? safeTop + contentSafeTop : 0;

  return (
    <div className="min-h-screen bg-tg-bg text-tg-text pb-20">
      {/* Safe area spacer for fullscreen mode */}
      {topPad > 0 && <div style={{ height: topPad }} className="bg-tg-bg" />}

      {/* Header — title + breadcrumbs + cart icons */}
      <header className="sticky z-50 bg-tg-bg border-b border-tg-hint/20" style={{ top: topPad }}>
        <div className={`px-4 ${isFullscreen ? 'pr-16' : ''}`}>
          {/* Top row: title + icons */}
          <div className="flex items-center justify-between h-11">
            {/* Spacer when Telegram native BackButton is visible */}
            {page !== 'catalog' && <div className="w-16 shrink-0" />}
            <h1 className="text-base font-semibold truncate flex-1">
              {getTitle()}
            </h1>
            {approved && (
              <div className="flex items-center gap-3 shrink-0 -mr-2">
                <button
                  onClick={() => navigateTo('cabinet')}
                  className="text-xl p-2"
                  title={t.cabinet}
                >
                  👤
                </button>
                <button
                  onClick={() => navigateTo('cart')}
                  className="relative text-xl p-2"
                >
                  🛒
                  {cart.totalCount > 0 && (
                    <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center">
                      {cart.totalCount}
                    </span>
                  )}
                </button>
              </div>
            )}
          </div>
          {/* Breadcrumb row — tappable path showing where the user is */}
          {getBreadcrumbs().length > 0 && (
            <div className="flex items-center gap-1 pb-2 -mt-1 overflow-x-auto scrollbar-hide">
              {getBreadcrumbs().map((crumb, i, arr) => (
                <span key={i} className="flex items-center gap-1 shrink-0">
                  {crumb.action ? (
                    <button
                      onClick={crumb.action}
                      className="text-xs text-tg-link active:underline"
                    >
                      {crumb.label}
                    </button>
                  ) : (
                    <span className="text-xs text-tg-hint">{crumb.label}</span>
                  )}
                  {i < arr.length - 1 && <span className="text-xs text-tg-hint/50">›</span>}
                </span>
              ))}
            </div>
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
        {(page === 'products' || page === 'product_detail') && (
          <ProductsPage
            category={selectedCategory}
            producer={selectedProducer}
            searchQuery={searchQuery}
            cart={cart}
            approved={approved}
            onSelectProduct={(product) => navigateTo('product_detail', product)}
            onSearch={(q) => navigateTo('search', q)}
          />
        )}
        {page === 'cart' && (
          <CartPage cart={cart} approved={approved} />
        )}
        {page === 'cabinet' && (
          <CabinetPage cart={cart} onNavigateToCart={() => navigateTo('cart')} />
        )}
      </main>

      {/* Product detail as full-screen overlay — preserves scroll position underneath */}
      {page === 'product_detail' && selectedProduct && (
        <div className="fixed inset-0 z-[90] bg-tg-bg overflow-y-auto">
          {/* Safe area spacer */}
          {topPad > 0 && <div style={{ height: topPad }} className="bg-tg-bg" />}
          {/* Overlay header with breadcrumbs */}
          <header className="sticky z-50 bg-tg-bg border-b border-tg-hint/20" style={{ top: topPad }}>
            <div className={`px-4 ${isFullscreen ? 'pr-16' : ''}`}>
              <div className="flex items-center justify-between h-11">
                {/* Spacer for native BackButton */}
                <div className="w-16 shrink-0" />
                <h1 className="text-base font-semibold truncate flex-1">
                  {selectedProduct?.name || t.all_products}
                </h1>
                {approved && (
                  <div className="flex items-center gap-3 shrink-0 -mr-2">
                    <button
                      onClick={() => navigateTo('cabinet')}
                      className="text-xl p-2"
                    >
                      👤
                    </button>
                    <button
                      onClick={() => navigateTo('cart')}
                      className="relative text-xl p-2"
                    >
                      🛒
                      {cart.totalCount > 0 && (
                        <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center">
                          {cart.totalCount}
                        </span>
                      )}
                    </button>
                  </div>
                )}
              </div>
              {/* Breadcrumb for product detail */}
              {getBreadcrumbs().length > 0 && (
                <div className="flex items-center gap-1 pb-2 -mt-1 overflow-x-auto scrollbar-hide">
                  {getBreadcrumbs().map((crumb, i, arr) => (
                    <span key={i} className="flex items-center gap-1 shrink-0">
                      {crumb.action ? (
                        <button
                          onClick={crumb.action}
                          className="text-xs text-tg-link active:underline"
                        >
                          {crumb.label}
                        </button>
                      ) : (
                        <span className="text-xs text-tg-hint">{crumb.label}</span>
                      )}
                      {i < arr.length - 1 && <span className="text-xs text-tg-hint/50">›</span>}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </header>
          <div className="px-3 py-3">
            <ProductDetailPage
              product={selectedProduct}
              producer={selectedProducer}
              cart={cart}
              approved={approved}
              onBack={goBack}
            />
          </div>
        </div>
      )}
    </div>
  );
}
