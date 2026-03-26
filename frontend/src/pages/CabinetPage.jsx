import { useState, useEffect } from 'react';
import { formatCartPrice } from '../utils/api';
import t from '../i18n/uz.json';

const API = '/api/cabinet';

function getTelegramUserId() {
  return window.Telegram?.WebApp?.initDataUnsafe?.user?.id || 0;
}

const STATUS_LABELS = {
  submitted: t.order_status_submitted,
  confirmed: t.order_status_confirmed,
  delivered: t.order_status_delivered,
  completed: t.order_status_completed,
};

const STATUS_ICONS = {
  submitted: '📤',
  confirmed: '✅',
  delivered: '🚚',
  completed: '✔️',
};

function formatDate(dateStr) {
  if (!dateStr) return '';
  try {
    const d = new Date(dateStr + 'Z'); // UTC from SQLite
    return d.toLocaleDateString('uz-UZ', {
      day: 'numeric',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return dateStr;
  }
}

export default function CabinetPage({ cart, onNavigateToCart }) {
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState(null);
  const [expandedItems, setExpandedItems] = useState([]);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [reorderDialog, setReorderDialog] = useState(null); // order_id or null
  const [reordering, setReordering] = useState(false);
  const [toast, setToast] = useState(null);

  const userId = getTelegramUserId();

  // Load orders list
  useEffect(() => {
    if (!userId) {
      setLoading(false);
      return;
    }
    fetch(`${API}/orders?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        setOrders(data.orders || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [userId]);

  // Toggle expand — load detail when expanding
  const toggleExpand = async (orderId) => {
    if (expandedId === orderId) {
      setExpandedId(null);
      setExpandedItems([]);
      return;
    }

    setExpandedId(orderId);
    setLoadingDetail(true);
    try {
      const res = await fetch(`${API}/orders/${orderId}?telegram_id=${userId}`);
      const data = await res.json();
      setExpandedItems(data.items || []);
    } catch {
      setExpandedItems([]);
    }
    setLoadingDetail(false);
  };

  // Reorder flow
  const handleReorderTap = (orderId) => {
    if (cart.items.length > 0) {
      setReorderDialog(orderId);
    } else {
      doReorder(orderId, 'replace');
    }
  };

  const doReorder = async (orderId, mode) => {
    setReorderDialog(null);
    setReordering(true);
    try {
      const res = await fetch(
        `${API}/orders/${orderId}/reorder?telegram_id=${userId}&mode=${mode}`,
        { method: 'POST' }
      );
      const data = await res.json();
      if (data.ok) {
        await cart.reloadCart();
        let msg = t.reorder_success;
        if (data.skipped > 0) {
          msg += ` (${data.skipped} ${t.reorder_partial})`;
        }
        setToast(msg);
        setTimeout(() => {
          setToast(null);
          onNavigateToCart?.();
        }, 1500);
      }
    } catch {
      setToast('Xatolik yuz berdi');
      setTimeout(() => setToast(null), 2000);
    }
    setReordering(false);
  };

  if (loading) {
    return <div className="text-center py-16 text-tg-hint">{t.loading}</div>;
  }

  if (orders.length === 0) {
    return (
      <div className="text-center py-16">
        <div className="text-5xl mb-4">🏛️</div>
        <div className="text-lg font-medium">{t.no_orders}</div>
        <div className="text-sm text-tg-hint mt-1">{t.no_orders_desc}</div>
      </div>
    );
  }

  return (
    <div>
      <div className="text-sm text-tg-hint mb-3">
        {t.order_history} ({orders.length})
      </div>

      <div className="space-y-2">
        {orders.map(order => {
          const isExpanded = expandedId === order.id;

          return (
            <div key={order.id} className="bg-tg-secondary rounded-xl overflow-hidden">
              {/* Order summary row */}
              <button
                onClick={() => toggleExpand(order.id)}
                className="w-full text-left p-3 active:bg-tg-hint/10 transition-colors"
              >
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-semibold">
                        #{order.id}
                      </span>
                      <span className="text-xs px-2 py-0.5 rounded-full bg-tg-bg text-tg-hint">
                        {STATUS_ICONS[order.status]} {STATUS_LABELS[order.status] || order.status}
                      </span>
                    </div>
                    <div className="text-xs text-tg-hint mt-1">
                      {formatDate(order.created_at)} · {order.item_count} {t.items_count}
                    </div>
                  </div>
                  <div className="text-right ml-2">
                    {order.total_usd > 0 && (
                      <div className="text-sm font-bold">{formatCartPrice(order.total_usd, 'USD')}</div>
                    )}
                    {order.total_uzs > 0 && (
                      <div className="text-sm font-bold">{formatCartPrice(order.total_uzs, 'UZS')}</div>
                    )}
                    <div className="text-xs text-tg-hint mt-0.5">
                      {isExpanded ? '▲' : '▼'}
                    </div>
                  </div>
                </div>
              </button>

              {/* Expanded detail */}
              {isExpanded && (
                <div className="border-t border-tg-hint/20 px-3 pb-3">
                  {loadingDetail ? (
                    <div className="text-center py-4 text-tg-hint text-sm">{t.loading}</div>
                  ) : (
                    <>
                      {/* Items list */}
                      <div className="mt-2 space-y-1.5">
                        {expandedItems.map((item, idx) => (
                          <div key={idx} className="flex items-center gap-2 py-1">
                            <div className="flex-1 min-w-0">
                              <div className="text-xs font-medium truncate">{item.product_name}</div>
                              {item.producer_name && (
                                <div className="text-[10px] text-tg-hint">{item.producer_name}</div>
                              )}
                            </div>
                            <div className="text-xs text-tg-hint whitespace-nowrap">
                              {item.quantity} {item.unit}
                            </div>
                            <div className="text-xs font-semibold whitespace-nowrap min-w-[50px] text-right">
                              {formatCartPrice(item.price * item.quantity, item.currency)}
                            </div>
                          </div>
                        ))}
                      </div>

                      {/* Reorder button */}
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          handleReorderTap(order.id);
                        }}
                        disabled={reordering}
                        className="w-full mt-3 bg-tg-button text-tg-button-text rounded-xl py-2.5 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50"
                      >
                        {reordering ? t.loading : `🔄 ${t.reorder}`}
                      </button>
                    </>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Reorder confirmation dialog */}
      {reorderDialog && (
        <>
          <div
            className="fixed inset-0 bg-black/40 z-[100]"
            onClick={() => setReorderDialog(null)}
          />
          <div className="fixed bottom-0 left-0 right-0 z-[101] bg-tg-bg rounded-t-2xl p-5 pb-8 shadow-2xl">
            <div className="w-10 h-1 bg-tg-hint/30 rounded-full mx-auto mb-4" />
            <div className="text-center mb-5">
              <div className="text-base font-semibold">{t.reorder_cart_has_items}</div>
              <div className="text-xs text-tg-hint mt-1">
                {t.cart}: {cart.totalCount} {t.products_count}
              </div>
            </div>
            <div className="space-y-2.5">
              <button
                onClick={() => doReorder(reorderDialog, 'replace')}
                className="w-full bg-tg-button text-tg-button-text rounded-xl py-3 font-semibold text-sm"
              >
                🔄 {t.reorder_replace}
              </button>
              <button
                onClick={() => doReorder(reorderDialog, 'merge')}
                className="w-full bg-tg-secondary text-tg-text rounded-xl py-3 font-semibold text-sm"
              >
                ➕ {t.reorder_merge}
              </button>
              <button
                onClick={() => setReorderDialog(null)}
                className="w-full text-tg-hint text-sm py-2"
              >
                {t.reorder_cancel}
              </button>
            </div>
          </div>
        </>
      )}

      {/* Toast notification */}
      {toast && (
        <div className="fixed top-16 left-4 right-4 z-[200] bg-green-600 text-white rounded-xl py-3 px-4 text-center text-sm font-medium shadow-lg">
          {toast}
        </div>
      )}
    </div>
  );
}
