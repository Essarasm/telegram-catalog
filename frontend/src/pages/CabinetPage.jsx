import { useState, useEffect } from 'react';
import { formatCartPrice } from '../utils/api';
import t from '../i18n/uz.json';

const API = '/api/cabinet';
const FINANCE_API = '/api/finance';

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

const MONTHS = t.balance_month_short || ['Yan', 'Fev', 'Mar', 'Apr', 'May', 'Iyun', 'Iyul', 'Avg', 'Sen', 'Okt', 'Noy', 'Dek'];

function formatUzs(amount) {
  if (!amount && amount !== 0) return '0';
  const num = Math.round(Math.abs(amount));
  return num.toLocaleString('ru-RU').replace(/,/g, ' ');
}

function formatUsd(v) {
  return '$' + Math.abs(Math.round(v)).toLocaleString('en-US');
}

function formatPeriod(start) {
  if (!start) return '';
  try {
    const d = new Date(start);
    return `${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
  } catch {
    return start;
  }
}

function formatDate(dateStr) {
  if (!dateStr) return '';
  try {
    const d = new Date(dateStr + 'Z');
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
  const [reorderDialog, setReorderDialog] = useState(null);
  const [reordering, setReordering] = useState(false);
  const [toast, setToast] = useState(null);
  const [balance, setBalance] = useState(null);
  const [balanceLoading, setBalanceLoading] = useState(true);

  const userId = getTelegramUserId();

  // Load orders and balance
  useEffect(() => {
    if (!userId) {
      setLoading(false);
      setBalanceLoading(false);
      return;
    }
    fetch(`${API}/orders?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        setOrders(data.orders || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));

    fetch(`${FINANCE_API}/balance?telegram_id=${userId}`)
      .then(r => r.json())
      .then(data => {
        if (data.ok && data.has_balance) {
          setBalance(data.balance);
        }
        setBalanceLoading(false);
      })
      .catch(() => setBalanceLoading(false));
  }, [userId]);

  // Toggle expand order detail
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

  // ── Helper: balance status label ──
  const getBalanceLabel = (bal) => {
    if (bal > 0) return t.balance_debt;
    if (bal < 0) return t.balance_overpayment;
    return t.balance_settled;
  };

  // ── Balance Card — shows debt from дебиторка or fallback to оборотка ──
  const BalanceCard = () => {
    if (balanceLoading || !balance) return null;

    const debtUzs = balance.debt_uzs ?? null;
    const debtUsd = balance.debt_usd ?? null;
    const isDebtSource = debtUzs !== null;

    // Debt source (дебиторка) — simple debt display
    if (isDebtSource) {
      const hasUzs = debtUzs > 0;
      const hasUsd = debtUsd > 0;
      const isSettled = !hasUzs && !hasUsd;

      return (
        <div className="mb-4">
          <div className="text-sm text-tg-hint mb-2">{t.balance_title}</div>
          <div className="bg-tg-secondary rounded-xl p-4">
            <div className="text-[10px] text-tg-hint text-center mb-1">{t.balance_current}</div>

            {isSettled ? (
              <div className="text-center mb-2">
                <div className="text-xl font-bold text-tg-hint">{t.balance_settled}</div>
              </div>
            ) : (
              <div className="flex items-baseline justify-center gap-4 mb-2">
                {hasUzs && (
                  <div className="text-center">
                    <div className="text-xl font-bold text-red-500">
                      {formatUzs(debtUzs)} {t.balance_currency || "so'm"}
                    </div>
                  </div>
                )}
                {hasUzs && hasUsd && (
                  <div className="text-tg-hint/30 text-lg font-light">│</div>
                )}
                {hasUsd && (
                  <div className="text-center">
                    <div className="text-xl font-bold text-red-500">
                      {formatUsd(debtUsd)}
                    </div>
                  </div>
                )}
              </div>
            )}

            {!isSettled && (
              <div className="text-center text-xs text-red-400 mb-1">
                {t.balance_debt}
              </div>
            )}

            <div className="text-[10px] text-tg-hint text-center mt-2">
              {t.balance_updated}: {formatDate(balance.imported_at)}
            </div>
          </div>
        </div>
      );
    }

    // Fallback: оборотка-based balance (legacy)
    const currencies = balance.balances_by_currency || {};
    const uzs = currencies.UZS || {
      balance: balance.balance || 0,
      period_debit: balance.period_debit || 0,
      period_credit: balance.period_credit || 0,
      period_start: balance.period_start,
    };
    const usd = currencies.USD;
    const hasBoth = !!usd;

    const balUzs = uzs.balance || 0;
    const balUsd = usd ? (usd.balance || 0) : 0;

    const balColor = (v) => v === 0 ? 'text-tg-hint' : v > 0 ? 'text-red-500' : 'text-green-500';
    const fmtBal = (v, fn, suffix) => {
      if (v === 0) return <span className="text-tg-hint">0 {suffix}</span>;
      return <>{v > 0 ? '' : '−'}{fn(v)} {suffix}</>;
    };

    const hasAnyActivity =
      (uzs.period_debit || 0) > 0 || (uzs.period_credit || 0) > 0 ||
      (usd && ((usd.period_debit || 0) > 0 || (usd.period_credit || 0) > 0));

    return (
      <div className="mb-4">
        <div className="text-sm text-tg-hint mb-2">{t.balance_title}</div>
        <div className="bg-tg-secondary rounded-xl p-4">
          <div className="text-[10px] text-tg-hint text-center mb-1">{t.balance_current}</div>
          {hasBoth ? (
            <div className="flex items-baseline justify-center gap-4 mb-3">
              <div className="text-center">
                <div className={`text-xl font-bold ${balColor(balUzs)}`}>
                  {fmtBal(balUzs, formatUzs, t.balance_currency || "so'm")}
                </div>
              </div>
              <div className="text-tg-hint/30 text-lg font-light">│</div>
              <div className="text-center">
                <div className={`text-xl font-bold ${balColor(balUsd)}`}>
                  {fmtBal(balUsd, formatUsd, '')}
                </div>
              </div>
            </div>
          ) : (
            <div className="text-center mb-3">
              <div className={`text-xl font-bold ${balColor(balUzs)}`}>
                {fmtBal(balUzs, formatUzs, t.balance_currency || "so'm")}
              </div>
            </div>
          )}

          {hasAnyActivity ? (
            <div className="border-t border-tg-hint/20 pt-2">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-tg-hint">
                    <th className="text-left font-normal pb-1"></th>
                    <th className="text-right font-normal pb-1">UZS</th>
                    {hasBoth && <th className="text-right font-normal pb-1">USD</th>}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="text-tg-hint py-0.5">{t.balance_shipped}</td>
                    <td className="text-right font-semibold py-0.5">{formatUzs(uzs.period_debit || 0)}</td>
                    {hasBoth && <td className="text-right font-semibold py-0.5">{formatUsd(usd.period_debit || 0)}</td>}
                  </tr>
                  <tr>
                    <td className="text-tg-hint py-0.5">{t.balance_paid}</td>
                    <td className="text-right font-semibold text-green-600 py-0.5">{formatUzs(uzs.period_credit || 0)}</td>
                    {hasBoth && <td className="text-right font-semibold text-green-600 py-0.5">{formatUsd(usd.period_credit || 0)}</td>}
                  </tr>
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center text-xs text-tg-hint py-1">
              {t.balance_no_activity}
            </div>
          )}

          <div className="text-[10px] text-tg-hint text-center mt-2">
            {t.balance_updated}: {formatDate(balance.imported_at)}
          </div>
        </div>
      </div>
    );
  };

  // ── Last order only ──
  const lastOrder = orders.length > 0 ? orders[0] : null;
  const isExpanded = lastOrder && expandedId === lastOrder.id;

  if (!lastOrder && !balance) {
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
      <BalanceCard />

      {lastOrder && (
        <>
          <div className="text-sm text-tg-hint mb-2">{t.order_history}</div>

          <div className="bg-tg-secondary rounded-xl overflow-hidden">
            {/* Order summary row */}
            <button
              onClick={() => toggleExpand(lastOrder.id)}
              className="w-full text-left p-3 active:bg-tg-hint/10 transition-colors"
            >
              <div className="flex items-center justify-between">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold">
                      #{lastOrder.id}
                    </span>
                    <span className="text-xs px-2 py-0.5 rounded-full bg-tg-bg text-tg-hint">
                      {STATUS_ICONS[lastOrder.status]} {STATUS_LABELS[lastOrder.status] || lastOrder.status}
                    </span>
                  </div>
                  <div className="text-xs text-tg-hint mt-1">
                    {formatDate(lastOrder.created_at)} · {lastOrder.item_count} {t.items_count}
                  </div>
                </div>
                <div className="text-right ml-2">
                  {lastOrder.total_usd > 0 && (
                    <div className="text-sm font-bold">{formatCartPrice(lastOrder.total_usd, 'USD')}</div>
                  )}
                  {lastOrder.total_uzs > 0 && (
                    <div className="text-sm font-bold">{formatCartPrice(lastOrder.total_uzs, 'UZS')}</div>
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

                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleReorderTap(lastOrder.id);
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
        </>
      )}

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
